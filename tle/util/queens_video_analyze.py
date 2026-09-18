"""Estimate a LinkedIn Queens solve time with decimals from a screen recording.

Backs ``;queens time``.  LinkedIn only shows mm:ss, but every flip of the seconds
digit is a sample of its internal clock.  We:
  1. stream-decode the video once at low resolution, keeping only which 4x4 blocks
     changed between consecutive frames plus exact per-frame timestamps
     (``queens_video_decode``);
  2. find the timer automatically: the blocks that change at a ~1.0 s cadence
     (``queens_video_timer``);
  3. detect every flip on a full-resolution strip of the timer row, anchor the
     absolute count (the tens digit rolls only at multiples of 10) and read the
     final digits with templates of LinkedIn's timer font;
  4. find the finishing move: the last board change before Reset greys out, after
     the last timer flip (the timer freezes on solve);
  5. time = seconds shown + (t_finish - t_last_flip); the fitted clock phase over
     all flips is reported as a cross-check.

The video is decoded ~1.5 times in total: pass 1 stops as soon as it has located
the timer; pass 2 decodes once more producing both the low-res block changes and
the timer strip from the same decoder (no frame is ever stored).  About 20-50x
real time.  All pixel distances are expressed in units of the video width (one
unit ~ one timer digit), dark themes are inverted automatically.

CLI: ``python -m tle.util.queens_video_analyze VIDEO [VIDEO ...] [--debug] [--no-ocr]``
"""
import json
import os
import sys

import numpy as np

from tle.util.queens_video_decode import (
    BLOCK, SCALE_W, VideoTooLongError, decode, decode_strip, probe_frame_count,
)
from tle.util.queens_video_timer import (
    SCENE_FRACTION, MIN_FLIPS, find_timer_blocks, load_templates, longest_chain,
    merge_runs, normalise_polarity, read_timer, segment_timer,
)

EARLY_MIN_CHAIN = 6     # flips needed before trusting an early timer location during streaming


def format_solve_time(seconds, decimals=2):
    """41.83 -> '0:41.83'; 3725.4 -> '1:02:05.40'."""
    total = round(float(seconds), decimals)
    whole = int(total)
    frac = total - whole
    h, rem = divmod(whole, 3600)
    m, s = divmod(rem, 60)
    sec = f'{s:02d}' + (f'{frac:.{decimals}f}'[1:] if decimals else '')
    return f'{h}:{m:02d}:{sec}' if h else f'{m}:{sec}'


def _strip_geometry(timer_mask, w0, h0):
    ys, xs = np.nonzero(timer_mask)
    unit = BLOCK * w0 / SCALE_W
    box = (int(xs.min() * unit), int(ys.min() * unit),
           int((xs.max() + 1) * unit), int((ys.max() + 1) * unit))
    sx0, sy0 = 0, max(0, int(box[1] - 0.75 * unit)) // 2 * 2
    sw = min(w0, int(box[2] + 0.75 * unit)) // 2 * 2
    sh = (min(h0, int(box[3] + 0.75 * unit)) - sy0) // 2 * 2
    return box, unit, (sx0, sy0, sw, sh)


def _decode_with_timer(path, deadline=None):
    """Pass 1 streams the downscaled video and stops as soon as a solid chain of timer
    flips pins the timer's location. Pass 2 then decodes the video once, producing both
    the downscaled block changes and the full-resolution timer strip. (A clip too short
    to locate the timer early simply completes pass 1 and decodes the strip separately.)"""
    early = {'attempts': 0}

    def locate(ch_part, ts_part):
        early['attempts'] += 1
        if early['attempts'] > 20:
            return False
        sc = ch_part.mean(axis=(1, 2)) > SCENE_FRACTION
        tm = find_timer_blocks(ch_part, ts_part, sc, min_chain=EARLY_MIN_CHAIN)
        if tm.sum() == 0:
            return False
        early['timer'] = tm
        return True

    changed, ts, (w0, h0), strip = decode(path, SCALE_W, on_progress=locate, deadline=deadline)
    geo = None
    if 'timer' in early:
        _, _, geo = _strip_geometry(early['timer'], w0, h0)
        changed, ts, (w0, h0), strip = decode(path, SCALE_W, strip_geo=geo, deadline=deadline)
    return changed, ts, (w0, h0), strip, geo


def _anchor_flips(strip, flip_idx, flip_t, k, x_units, unit):
    """Absolute anchoring from the full-resolution strip. For each flip, the leftmost
    changed pixel column tells what rolled: units digit only, the tens digit too
    (...9 -> ...0, so seconds = 0 mod 10), or everything incl. the clock icon ("0:00"
    appearing). Returns (sec_at_flip, kind, leftmost, x_units, anchor)."""
    leftmost = []
    for i in flip_idx:
        dpx = np.abs(strip[i].astype(np.int16) - strip[i - 1].astype(np.int16)) > 40
        cols = np.nonzero(dpx.any(axis=0))[0]
        leftmost.append(int(cols.min()) if len(cols) else x_units)
    leftmost = np.array(leftmost)
    # measure where the minutes / tens / units glyphs actually sit (falls back to block geometry)
    segs = segment_timer(strip[flip_idx[-1]], x_units, unit)
    if len(segs) >= 3:
        m0, t0_, u0 = segs[-3][0], segs[-2][0], segs[-1][0]
        units_thr, tens_thr = (t0_ + u0) / 2, (m0 + t0_) / 2
        x_units = u0
    else:
        units_thr, tens_thr = x_units - 0.35 * unit, x_units - 1.6 * unit
    kind = np.where(leftmost < tens_thr, 'all', np.where(leftmost < units_thr, 'tens', 'units'))
    # "0:00" appearing: an 'all' event among the first few; everything up to it is pre-game
    appear = [i for i in range(min(3, len(kind)))
              if kind[i] == 'all' and (i == 0 or flip_t[i] - flip_t[0] < 2.5)]
    if appear:
        a = appear[-1]
        sec_at_flip = k - k[a]
        anchor = 'start seen (0:00 appears)'
    else:
        sec_at_flip = 1 + k
        anchor = 'assumed first flip = 0:01'
    tens = [int(v) for v, kd in zip(sec_at_flip, kind) if kd != 'units' and v > 0]
    if tens:
        shifts = [c for c in sorted(range(-9, 10), key=abs) if all((v + c) % 10 == 0 for v in tens)]
        if shifts:
            if shifts[0] != 0:
                sec_at_flip = sec_at_flip + shifts[0]
                anchor += f', corrected by {shifts[0]:+d}s via tens digit'
            else:
                anchor += ', tens digit confirms'
        else:
            anchor += ', tens digit INCONSISTENT'
    else:
        anchor += ', no tens roll to check'
    return sec_at_flip, kind, leftmost, x_units, anchor


# Failures that mean "the timer was not where we looked", worth a second pass with
# the neighbourhood-pooled timer search (see ``find_timer_blocks(dilate=True)``).
_RETRY_ERRORS = ('no timer-like region found', 'timer flips found', 'no usable timer flips')


def analyze(path, debug=False, _return_strip=False, templates_path=None, use_ocr=True,
            deadline=None, max_frames=None):
    """Return a result dict; on failure it carries an ``error`` key instead of ``time_s``.
    ``deadline`` (``time.monotonic()``) bounds the ffmpeg work; see ``VideoDecodeTimeout``.
    ``max_frames`` refuses longer clips with ``VideoTooLongError`` before decoding,
    since peak memory grows with the frame count (see ``probe_frame_count``)."""
    if max_frames is not None:
        frames = probe_frame_count(path, deadline)
        if frames is not None and frames > max_frames:
            raise VideoTooLongError(frames, max_frames)
    result = _analyze(path, debug, _return_strip, templates_path, use_ocr, deadline)
    if 'error' in result and any(k in result['error'] for k in _RETRY_ERRORS):
        retry = _analyze(path, debug, _return_strip, templates_path, use_ocr, deadline,
                         dilate=True)
        if 'error' not in retry:
            retry['anchor'] = 'small digits (pooled timer search); ' + retry['anchor']
            return retry
        # A 2-second solve has exactly two ticks (0:00 appears inside the board's
        # own scene transition), one short of a chain.  Accept a two-event chain
        # only when the digit OCR independently reads the same count.
        if use_ocr and any(k in retry['error'] for k in _RETRY_ERRORS):
            two = _analyze(path, debug, _return_strip, templates_path, use_ocr, deadline,
                           dilate=True, min_events=2)
            if 'error' not in two:
                two['anchor'] = 'two-tick solve (OCR-confirmed); ' + two['anchor']
                return two
    return result


def _analyze(path, debug, _return_strip, templates_path, use_ocr, deadline, dilate=False,
             min_events=MIN_FLIPS):
    decoded = _decode_with_timer(path, deadline)
    changed, ts, _, _, _ = decoded
    scene = changed.mean(axis=(1, 2)) > SCENE_FRACTION       # per transition
    if min_events >= MIN_FLIPS:
        candidates = [find_timer_blocks(changed, ts, scene, dilate=dilate)]
    else:   # two-event chains rank poorly (the status bar clock ticks too): try several
        candidates = find_timer_blocks(changed, ts, scene, dilate=dilate,
                                       min_events=min_events, ranked=True)
    result = {'file': path, 'error': 'no timer-like region found'}
    for timer in candidates:
        if timer.sum() == 0:
            continue
        result = _measure(path, decoded, scene, timer, debug, _return_strip, templates_path,
                          use_ocr, deadline, min_events)
        if 'error' not in result:
            break
    return result


def _measure(path, decoded, scene, timer, debug, _return_strip, templates_path, use_ocr,
             deadline, min_events):
    """Everything after the timer blocks are known: flips, anchoring, finish, OCR."""
    changed, ts, (w0, h0), strip, early_geo = decoded
    # full-resolution strip of the timer row; flips are detected on it (pixel-accurate).
    # `unit` = one analysis block in source pixels ~ one timer digit width; every pixel
    # distance below is expressed in it so the script is independent of screen resolution.
    timer_box, unit, geo = _strip_geometry(timer, w0, h0)
    x_units = timer_box[0]
    sx0, sy0, sw, sh = geo
    if strip is None or early_geo != geo:         # no (or a wrong) early location
        strip = decode_strip(path, sx0, sy0, sw, sh, deadline=deadline)
    strip = normalise_polarity(strip)
    sw, sh = strip.shape[2], strip.shape[1]
    n = min(len(strip), len(ts))
    strip, ts = strip[:n], ts[:n]
    changed, scene = changed[: n - 1], scene[: n - 1]
    dcol0, dcol1 = max(0, int(x_units - 3.5 * unit)), min(sw, int(x_units + 2.8 * unit))
    sd = np.abs(np.diff(strip[:, :, dcol0:dcol1].astype(np.int16), axis=0)) > 40
    strip_change = sd.sum(axis=(1, 2)) >= 15 * (unit / 16) ** 2
    flip_mask = strip_change & ~scene
    flip_idx = np.array(merge_runs(np.nonzero(flip_mask)[0])) + 1      # frame where new digit is visible
    flip_t = ts[flip_idx]
    start, length = longest_chain(flip_t)
    lo, hi = start, start + length + 1
    # a missed flip (2-3 s step) is only credible inside the chain, never at its ends
    while hi - lo >= 2 and round(flip_t[lo + 1] - flip_t[lo]) > 1:
        lo += 1
    while hi - lo >= 2 and round(flip_t[hi - 1] - flip_t[hi - 2]) > 1:
        hi -= 1
    flip_t = flip_t[lo:hi]
    flip_idx = flip_idx[lo:hi]
    if len(flip_t) < min_events:
        return {'file': path, 'error': f'only {len(flip_t)} timer flips found'}
    steps = np.concatenate([[0], np.round(np.diff(flip_t)).astype(int)])
    k = np.cumsum(steps)                       # 0,1,2,... seconds relative to first kept flip

    sec_at_flip, kind, leftmost, x_units, anchor = _anchor_flips(
        strip, flip_idx, flip_t, k, x_units, unit)
    keep = sec_at_flip > 0
    flip_t, flip_idx, sec_at_flip = flip_t[keep], flip_idx[keep], sec_at_flip[keep]
    kind, leftmost = kind[keep], leftmost[keep]
    if len(flip_t) < 1:
        return {'file': path, 'error': 'no usable timer flips'}
    # least-squares clock phase: flip_t ~ t0 + sec_at_flip
    t0 = float(np.mean(flip_t - sec_at_flip))
    phase_resid = flip_t - sec_at_flip - t0

    # finishing move. After the solve LinkedIn greys out Reset (same row as the timer, right side)
    # a frame or two after the last queen is drawn. Use that as the solve confirmation and take the
    # last board change just before it (the queen being drawn).
    last_i, last_t = flip_idx[-1], flip_t[-1]
    blk = BLOCK * w0 / SCALE_W
    hdr_lo, hdr_hi = int(timer_box[1] / blk), int(timer_box[3] / blk) + 1
    reset = np.zeros(timer.shape, bool)
    reset[hdr_lo:hdr_hi, int(timer.shape[1] * 0.6):] = True
    board = ~timer
    board[:hdr_hi, :] = False
    # the timer may have changed after the last *chained* flip (missed flips): anchor the finish
    # search on the very last timer change instead
    templates = load_templates(templates_path) if use_ocr else None
    all_changes = np.nonzero(flip_mask)[0] + 1
    last_i2, last_t2 = int(last_i), float(last_t)
    prev_ocr = read_timer(strip[last_i2], x_units, unit, templates) if templates else None
    extended = 0
    for e in all_changes[all_changes > last_i2]:
        gap = ts[e] - last_t2
        if gap < 0.5:
            continue
        if gap > 3.5:
            break
        cur = read_timer(strip[e], x_units, unit, templates) if templates else None
        if templates:
            if cur is None or prev_ocr is None or cur != prev_ocr + int(round(gap)):
                continue                      # misread / unrelated change: keep looking up to 3.5 s
        elif abs(gap - round(gap)) > 0.4:
            continue
        last_i2, last_t2, prev_ocr, extended = int(e), float(ts[e]), cur, extended + 1
    if extended:
        anchor += f'; chain extended by {extended} flips via OCR'
    win = np.nonzero((ts[1:] > last_t2) & (ts[1:] < last_t2 + 1.35))[0]
    board_changes = []
    grey_i = None
    for i in win:
        if scene[i]:
            break
        if changed[i][reset].any():
            grey_i = i + 1
            break
        if changed[i][board].sum() >= 2:
            board_changes.append(i + 1)
    if grey_i is not None:
        cands = [j for j in board_changes if ts[grey_i] - ts[j] <= 0.5]
        finish_i = cands[-1] if cands else grey_i
        finish_kind = 'queen drawn' if cands else 'reset greyed (no board change seen)'
    else:
        finish_i = board_changes[-1] if board_changes else None
        finish_kind = 'last board change (no grey-out seen)'
    if finish_i is None:
        return {'file': path, 'error': 'no finishing move found after last flip',
                'seconds_at_last_flip': int(sec_at_flip[-1])}
    finish_t = ts[finish_i]
    ocr = read_timer(strip[min(finish_i, len(strip) - 1)], x_units, unit, templates) if templates else None
    ocr_last = read_timer(strip[last_i2], x_units, unit, templates) if templates else None
    counted = int(sec_at_flip[-1])
    method = 'last flip + delta'
    if min_events < MIN_FLIPS and not (ocr is not None and ocr == ocr_last == counted):
        return {'file': path, 'error': f'only {len(flip_t)} timer flips found and OCR '
                                       f'({ocr}) does not confirm {counted}s'}
    if ocr is not None and ocr == ocr_last:
        if ocr != counted:
            anchor += f'; OCR reads {ocr}s (counted {counted}s) -> using OCR'
            sec_at_flip = sec_at_flip + (ocr - counted)
        else:
            anchor += '; OCR agrees'
    elif ocr is not None:
        # the timer flipped again between the last detected flip and the finish (missed flip):
        # take the whole seconds from OCR and the fraction from the fitted clock phase
        anchor += f'; OCR finish {ocr}s vs last flip {ocr_last}s -> OCR seconds + phase fraction'
        method = 'OCR + phase fraction'
    # frame-interval uncertainty: the true event happened somewhere in (prev frame, this frame]
    f_dt = finish_t - ts[finish_i - 1]
    l_dt = last_t2 - ts[last_i2 - 1]
    time_fit = finish_t - t0
    if method == 'OCR + phase fraction':
        time_local = ocr + ((finish_t - t0) % 1.0)
        time_fit = time_local
    else:
        time_local = (ocr if ocr is not None else sec_at_flip[-1]) + (finish_t - last_t2)
    result = {
        'file': path,
        'ocr_seconds': ocr,
        'method': method,
        'time_s': round(float(time_local), 3),
        'time_s_fit': round(float(time_fit), 3),
        'uncertainty_s': round(float((f_dt + l_dt) / 2), 3),
        'seconds_shown': int(ocr if ocr is not None else sec_at_flip[-1]),
        'flips': int(len(flip_t)),
        'phase_residual_ms_max': round(float(np.abs(phase_resid).max() * 1000), 1),
        'frame_interval_ms': round(float(np.median(np.diff(ts)) * 1000), 1),
        'anchor': anchor,
        'timer_box': timer_box,
        'video': [int(w0), int(h0)],
        'digit_unit_px': round(float(unit), 1),
        'finish_frame': int(finish_i),
        'finish_kind': finish_kind,
        'grey_frame': int(grey_i) if grey_i is not None else None,
        'finish_video_t': round(float(finish_t), 3),
    }
    if _return_strip:
        result.update(_strip=strip, _x_units=x_units, _unit=unit, _flip_idx=[int(i) for i in flip_idx])
    if debug:
        result['flip_times'] = [round(float(t), 3) for t in flip_t]
        result['flip_seconds'] = [int(s) for s in sec_at_flip]
        result['flip_kind'] = [str(x) for x in kind]
        result['flip_leftmost_col'] = [int(x) for x in leftmost]
    return result


def summarize(r):
    """One-line human summary of an ``analyze`` result (used by the CLI)."""
    if 'error' in r:
        extra = f" (last flip {r['seconds_at_last_flip']}s)" if 'seconds_at_last_flip' in r else ''
        return f"ERROR {r['error']}{extra}"
    return (f"{r['time_s']:.2f} s  ±{r['uncertainty_s']:.3f}  "
            f"(shown {r['seconds_shown']}s, fit {r['time_s_fit']:.2f}s, {r['flips']} flips, "
            f"phase resid ≤{r['phase_residual_ms_max']}ms; {r['anchor']})")


def main(argv):
    debug = '--debug' in argv
    use_ocr = '--no-ocr' not in argv
    templates_path = None
    for a in argv:
        if a.startswith('--templates='):
            templates_path = a.split('=', 1)[1]
    paths = [a for a in argv if not a.startswith('--')]
    if not paths:
        print(__doc__)
        return 2
    for p in paths:
        try:
            r = analyze(p, debug, templates_path=templates_path, use_ocr=use_ocr)
        except Exception as e:  # keep going over a batch
            r = {'file': p, 'error': repr(e)}
        print(f'{os.path.basename(p)}: {summarize(r)}')
        if debug:
            print(json.dumps(r, indent=1, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
