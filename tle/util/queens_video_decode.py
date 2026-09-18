"""ffmpeg streaming decode for LinkedIn Queens screen recordings.

Part of the ``;queens time`` pipeline (see ``queens_video_analyze``).  This
module owns everything that talks to ffmpeg/ffprobe: it streams a downscaled
grayscale copy of the video, reduces every consecutive frame pair to a mask of
4x4 blocks that changed, and can additionally crop a full-resolution strip of
the timer row from the same decoder.  Screen recordings are variable-frame-rate,
so exact per-frame timestamps come from ``showinfo`` (integer pts times the
stream time base) rather than an assumed fps.
"""
import json
import os
import queue
import re
import subprocess
import tempfile
import threading
import time
from fractions import Fraction

import numpy as np

SCALE_W = 220           # analysis width in px (880 -> 220 = 4x downscale)
BLOCK = 4               # block size at analysis scale (16 px at full res)
BLOCK_CHANGE = 10.0     # mean abs gray diff for a block to count as "changed"

_PTS_RE = re.compile(rb'\bpts:\s*(-?\d+)')


class VideoDecodeError(RuntimeError):
    """ffmpeg/ffprobe failed or produced something we cannot use."""


class VideoDecodeTimeout(VideoDecodeError):
    """The caller's deadline passed while ffmpeg was still decoding."""


def _remaining(deadline):
    if deadline is None:
        return None
    left = deadline - time.monotonic()
    if left <= 0:
        raise VideoDecodeTimeout('analysis deadline passed')
    return left


def run(cmd, deadline=None):
    try:
        return subprocess.run(cmd, check=True, capture_output=True,
                              timeout=_remaining(deadline)).stdout
    except subprocess.TimeoutExpired as exc:
        raise VideoDecodeTimeout('analysis deadline passed') from exc
    except subprocess.CalledProcessError as exc:
        tail = (exc.stderr or b'')[-400:].decode('utf-8', 'replace').strip()
        raise VideoDecodeError(f'{cmd[0]} failed ({exc.returncode}): {tail}') from exc


def probe_stream(path, deadline=None):
    """(width, height, time_base) of the first video stream."""
    out = run(['ffprobe', '-v', 'error', '-select_streams', 'v:0',
               '-show_entries', 'stream=width,height,time_base', '-of', 'json', path],
              deadline=deadline)
    streams = json.loads(out).get('streams') or []
    if not streams:
        raise VideoDecodeError('no video stream found')
    st = streams[0]
    return int(st['width']), int(st['height']), Fraction(st['time_base'])


class VideoTooLongError(VideoDecodeError):
    """The clip has more frames than the caller allows (see ``probe_frame_count``)."""

    def __init__(self, frames, max_frames):
        super().__init__(f'{frames} frames exceeds the limit of {max_frames}')
        self.frames, self.max_frames = frames, max_frames


def probe_frame_count(path, deadline=None):
    """Best-effort frame count of the first video stream without decoding, or None.

    Peak memory of the analysis scales with the frame count (the full-resolution
    timer strip of every frame is held at once), which the upload-size cap does
    not bound: a 60 fps 1080p recording of a slow solve is several GB of strip.
    Uses the container's ``nb_frames`` when present (MP4/MOV always carry it),
    else ``duration * avg_frame_rate`` (the true average even for variable
    frame rate sources), else the format duration.  Anything unreadable yields
    None so an odd container is not refused outright."""
    out = run(['ffprobe', '-v', 'error', '-select_streams', 'v:0',
               '-show_entries', 'stream=nb_frames,duration,avg_frame_rate:format=duration',
               '-of', 'json', path], deadline=deadline)
    info = json.loads(out)
    streams = info.get('streams') or []
    if not streams:
        return None
    st = streams[0]
    try:
        return int(st['nb_frames'])
    except (KeyError, TypeError, ValueError):
        pass
    duration = st.get('duration') or (info.get('format') or {}).get('duration')
    try:
        fps = float(Fraction(st.get('avg_frame_rate') or '0'))
        duration = float(duration)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if fps <= 0 or duration <= 0:
        return None
    return int(round(duration * fps))


def frame_times(path, deadline=None):
    """Per-frame timestamps via ffprobe (fallback when showinfo lines go missing)."""
    out = run(['ffprobe', '-v', 'error', '-select_streams', 'v:0',
               '-show_entries', 'frame=pts_time,pkt_pts_time,best_effort_timestamp_time',
               '-of', 'json', path], deadline=deadline)
    frames = json.loads(out)['frames']
    ts = []
    for f in frames:
        v = f.get('pts_time') or f.get('best_effort_timestamp_time') or f.get('pkt_pts_time')
        ts.append(float(v))
    return np.array(ts)


def block_changes(a, b):
    """Boolean (n, hb, wb) mask of BLOCKxBLOCK blocks whose mean abs difference between
    frames a and b exceeds BLOCK_CHANGE. Exact integer arithmetic; the 4-pixel sums are done
    by re-viewing the uint8 rows as uint16/uint32 words (~3x faster than numpy axis sums)."""
    assert BLOCK == 4
    hb, wb = a.shape[1] // BLOCK, a.shape[2] // BLOCK
    a = a[:, : hb * BLOCK, : wb * BLOCK]
    b = b[:, : hb * BLOCK, : wb * BLOCK]
    d = np.ascontiguousarray(np.maximum(a, b) - np.minimum(a, b))     # |a-b|, uint8
    p = d.view(np.uint16)                                              # lo | hi<<8
    x2 = p & 0xFF
    x2 += p >> 8                                                       # pair sums (n, H, W/2)
    q = x2.view(np.uint32)
    x4 = q & 0xFFFF
    x4 += q >> 16                                                      # 4-pixel sums (n, H, wb)
    y = x4[:, 0::4] + x4[:, 1::4]
    y += x4[:, 2::4]
    y += x4[:, 3::4]                                                   # 4x4 block sums (n, hb, wb)
    return y > BLOCK_CHANGE * BLOCK * BLOCK


def _decode_command(path, width, h, strip_geo, strip_path):
    # ``-vsync passthrough`` rather than ``-fps_mode``: the latter only exists from
    # ffmpeg 5.1, and the Docker image (ubuntu:20.04) ships 4.2.  Newer ffmpeg
    # merely prints a deprecation warning, which the pts: regex ignores.
    scaled = f'showinfo=checksum=0,scale={width}:{h}'
    if strip_geo is None:
        return ['ffmpeg', '-v', 'info', '-nostats', '-i', path, '-vsync', 'passthrough',
                '-vf', scaled, '-pix_fmt', 'gray', '-f', 'rawvideo', '-']
    x0, y0, sw, sh = strip_geo
    # two outputs fed from one decoder (per-output -vf shares the decoded frames; a
    # filter_complex split would copy every full-resolution frame)
    return ['ffmpeg', '-v', 'info', '-nostats', '-y', '-i', path,
            '-map', '0:v', '-vsync', 'passthrough', '-vf', scaled,
            '-pix_fmt', 'gray', '-f', 'rawvideo', '-',
            '-map', '0:v', '-vsync', 'passthrough', '-vf', f'crop={sw}:{sh}:{x0}:{y0}',
            '-pix_fmt', 'gray', '-f', 'rawvideo', strip_path]


def decode(path, width=SCALE_W, chunk_frames=64, on_progress=None, strip_geo=None,
           deadline=None):
    """One streaming decode pass. Returns (changed, ts, (w0, h0), strip):
    changed[i] = block-change mask between frame i and i+1, ts = exact per-frame timestamps
    (from showinfo pts * time_base). Downscaled frames are never kept in memory.
    `on_progress(changed_so_far, ts_so_far)` is called every ~4 s of video; returning True
    stops the decode early (the partial results are returned).
    `strip_geo=(x0, y0, w, h)` additionally writes a full-resolution gray crop of every frame
    (the timer strip) from the same decode, returned as `strip`.
    `deadline` is a ``time.monotonic()`` value; passing it kills ffmpeg and raises
    ``VideoDecodeTimeout``."""
    w0, h0, tb = probe_stream(path, deadline)
    h = int(round(h0 * width / w0)) // 2 * 2
    fsize = width * h
    strip_file = None
    if strip_geo is not None:
        strip_geo = tuple(v // 2 * 2 for v in strip_geo)      # even: yuv420 crops round to even
        strip_file = tempfile.NamedTemporaryFile(suffix='.gray', delete=False)
        strip_file.close()
    cmd = _decode_command(path, width, h, strip_geo,
                          strip_file.name if strip_file else None)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError:
        if strip_file:
            os.unlink(strip_file.name)
        raise
    q = queue.Queue(maxsize=6)
    pts = []

    def reader():                                   # keeps ffmpeg's stdout pipe drained
        while True:
            try:
                buf = proc.stdout.read(fsize * chunk_frames)
            except ValueError:                      # pipe closed after an early stop
                buf = b''
            q.put(buf)
            if not buf:
                return

    def err_reader():                               # showinfo lines -> pts, as they arrive
        for line in proc.stderr:
            m = _PTS_RE.search(line)
            if m:
                pts.append(int(m.group(1)))
    rt = threading.Thread(target=reader, daemon=True)
    rt.start()
    et = threading.Thread(target=err_reader, daemon=True)
    et.start()
    changed, prev, n, since, stopped = [], None, 0, 0, False
    try:
        while True:
            try:
                buf = q.get(timeout=_remaining(deadline))
            except queue.Empty:
                raise VideoDecodeTimeout('analysis deadline passed')
            if not buf:
                break
            k = len(buf) // fsize
            if k == 0:
                break
            cur = np.frombuffer(buf, np.uint8, count=k * fsize).reshape(k, h, width)
            if prev is not None:
                changed.append(block_changes(prev[None], cur[:1]))
            if k > 1:
                changed.append(block_changes(cur[:-1], cur[1:]))
            prev = cur[-1].copy()
            n += k
            since += k
            if on_progress is not None and since >= 96 and len(pts) >= n:
                since = 0
                ts_part = np.array([float(Fraction(v) * tb) for v in pts[:n]])
                if on_progress(np.concatenate(changed, axis=0), ts_part):
                    stopped = True
                    proc.kill()
                    break
    finally:
        if proc.poll() is None:
            proc.kill()                             # deadline, early stop or an error
        rc = proc.wait()
        while rt.is_alive():                        # unpark a reader blocked on a full
            try:                                    # queue (it would otherwise keep up
                q.get(timeout=0.05)                 # to 7 chunks alive forever)
            except queue.Empty:
                pass
        proc.stdout.close()
        et.join()
    try:
        if rc != 0 and not stopped:
            raise VideoDecodeError(f'ffmpeg failed ({rc}) on {os.path.basename(path)}')
        if n < 2:
            raise VideoDecodeError('video too short')
        changed = np.concatenate(changed, axis=0)
        ts = np.array([float(Fraction(v) * tb) for v in pts[:n]])
        if len(ts) != n and not stopped:
            ts = frame_times(path, deadline)[:n]
        strip = None
        if strip_file is not None:
            _, _, sw, sh = strip_geo
            raw = np.fromfile(strip_file.name, np.uint8)
            if len(raw) % (sw * sh):
                raise VideoDecodeError(
                    f'strip decode size mismatch ({len(raw)} bytes for {sw}x{sh})')
            strip = raw.reshape(-1, sh, sw)
    finally:
        if strip_file is not None:
            os.unlink(strip_file.name)
    return changed, ts, (w0, h0), strip


def decode_strip(path, x0, y0, w, h, deadline=None):
    """Full-resolution grayscale crop of every frame (used for the timer strip)."""
    # yuv420 sources make ffmpeg round odd crop sizes down to even, which would misalign
    # every frame: keep all four values even ourselves
    x0, y0, w, h = (v // 2 * 2 for v in (x0, y0, w, h))
    raw = run(['ffmpeg', '-v', 'error', '-i', path, '-vsync', 'passthrough',
               '-vf', f'crop={w}:{h}:{x0}:{y0}', '-pix_fmt', 'gray', '-f', 'rawvideo', '-'],
              deadline=deadline)
    if len(raw) % (w * h):
        raise VideoDecodeError(f'strip decode size mismatch ({len(raw)} bytes for {w}x{h})')
    n = len(raw) // (w * h)
    return np.frombuffer(raw, np.uint8).reshape(n, h, w)
