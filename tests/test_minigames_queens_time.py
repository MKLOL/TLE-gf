"""``;queens time``: solve-time-from-video command wiring (analysis is mocked)."""
import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs import _mgimpl_queenstime as queenstime_module
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs.minigames import Minigames, MinigameCogError
from tle.util import queens_video_analyze
from tle.util import queens_video_decode
from tle.util.queens_video_decode import (
    VideoDecodeError, VideoDecodeTimeout, VideoTooLongError,
)

from tests.minigames_test_utils import _FakeAttachment, _FakeChannel


class _VideoAttachment(_FakeAttachment):
    def __init__(self, filename='solve.mp4', payload=b'\x00' * 16, content_type='video/mp4',
                 size=None):
        super().__init__(filename, payload)
        self.content_type = content_type
        if size is not None:
            self.size = size


def _make_ctx(attachments=(), reply_to=None, resolved=True):
    sent = []

    async def send(content=None, *, embed=None, **kwargs):
        sent.append({'content': content, 'embed': embed, 'kwargs': kwargs})

    channel = _FakeChannel(200)
    fetched = {}

    async def fetch_message(message_id):
        if message_id not in fetched:
            raise RuntimeError('not found')
        return fetched[message_id]

    channel.fetch_message = fetch_message
    reference = None
    if reply_to is not None:
        reference = SimpleNamespace(message_id=reply_to.id,
                                    resolved=reply_to if resolved else None)
        fetched[reply_to.id] = reply_to
    message = SimpleNamespace(id=1, attachments=list(attachments), reference=reference)
    return SimpleNamespace(message=message, channel=channel, send=send, sent=sent,
                           author=SimpleNamespace(id=10))


def _source_message(attachments, author_id=20):
    return SimpleNamespace(
        id=2, attachments=list(attachments),
        author=SimpleNamespace(id=author_id, mention=f'<@{author_id}>'))


_OK_RESULT = {
    'time_s': 44.75, 'time_s_fit': 44.85, 'uncertainty_s': 0.042, 'seconds_shown': 44,
    'flips': 44, 'phase_residual_ms_max': 124.2, 'frame_interval_ms': 33.4,
    'anchor': 'start seen (0:00 appears), tens digit confirms; OCR agrees',
    'finish_kind': 'queen drawn', 'ocr_seconds': 44, 'method': 'last flip + delta',
}


def _run(cog, ctx):
    return asyncio.run(Minigames.queens_time.__wrapped__(cog, ctx))


class TestFormatSolveTime:
    @pytest.mark.parametrize('seconds, text', [
        (44.75, '0:44.75'), (5.0, '0:05.00'), (61.234, '1:01.23'), (599.999, '10:00.00'),
        (3725.4, '1:02:05.40'), (0.0, '0:00.00'),
    ])
    def test_two_decimals(self, seconds, text):
        assert queens_video_analyze.format_solve_time(seconds) == text

    def test_no_decimals(self):
        assert queens_video_analyze.format_solve_time(44, decimals=0) == '0:44'
        assert queens_video_analyze.format_solve_time(125, decimals=0) == '2:05'


class TestPickVideoAttachment:
    def test_prefers_first_video_by_content_type_or_extension(self):
        image = _FakeAttachment('shot.png', b'x')
        image.content_type = 'image/png'
        octet = _VideoAttachment('Screenrecorder.MP4', content_type='application/octet-stream')
        msg = SimpleNamespace(attachments=[image, octet])
        assert queenstime_module.pick_video_attachment(msg) is octet

    def test_none_without_video(self):
        assert queenstime_module.pick_video_attachment(SimpleNamespace(attachments=[])) is None
        assert queenstime_module.pick_video_attachment(SimpleNamespace()) is None

    def test_safe_filename(self):
        att = SimpleNamespace(filename='../../we ird (1).MOV')
        assert queenstime_module._safe_video_filename(att) == 'we_ird__1_.mov'
        assert queenstime_module._safe_video_filename(SimpleNamespace(filename='x.txt')) == 'x.mp4'


class TestQueensTimeCommand:
    @pytest.fixture(autouse=True)
    def _ffmpeg_present(self, monkeypatch):
        monkeypatch.setattr(queens_video_decode, 'find_tool', lambda tool: '/usr/bin/' + tool)

    def _patch_analyze(self, monkeypatch, result=None, exc=None, seen=None):
        def fake_analyze(path, deadline=None, max_frames=None, **kwargs):
            if seen is not None:
                seen['path'] = path
                seen['deadline'] = deadline
                seen['max_frames'] = max_frames
                with open(path, 'rb') as fh:
                    seen['payload'] = fh.read()
            if exc is not None:
                raise exc
            return dict(result or _OK_RESULT)
        monkeypatch.setattr(queens_video_analyze, 'analyze', fake_analyze)

    def test_missing_ffmpeg_detected_before_download(self, monkeypatch):
        seen = {}
        self._patch_analyze(monkeypatch, seen=seen)
        monkeypatch.setattr(queens_video_decode, 'find_tool',
                            lambda tool: None if tool == 'ffprobe' else '/usr/bin/' + tool)
        ctx = _make_ctx(reply_to=_source_message([_VideoAttachment()]))
        with pytest.raises(MinigameCogError, match='`ffprobe` was not found'):
            _run(Minigames(bot=None), ctx)
        assert seen == {}

    def test_reply_with_video_reports_time(self, monkeypatch):
        seen = {}
        self._patch_analyze(monkeypatch, seen=seen)
        source = _source_message([_VideoAttachment(payload=b'MP4DATA')])
        ctx = _make_ctx(reply_to=source)
        _run(Minigames(bot=None), ctx)
        assert seen['payload'] == b'MP4DATA'
        assert seen['deadline'] is not None
        assert seen['max_frames'] == queenstime_module._QUEENS_VIDEO_MAX_FRAMES
        assert seen['path'].endswith('solve.mp4')
        embed = ctx.sent[-1]['embed']
        assert embed.title == 'LinkedIn Queens solve time'
        assert '**0:44.75**' in embed.description
        assert '±0.04 s' in embed.description
        assert '`0:44`' in embed.description
        assert '<@20>' in embed.description
        assert 'Lower confidence' not in embed.description
        assert '44 timer flips' in embed.footer['text']

    def test_unresolved_reply_is_fetched(self, monkeypatch):
        self._patch_analyze(monkeypatch)
        source = _source_message([_VideoAttachment()])
        ctx = _make_ctx(reply_to=source, resolved=False)
        _run(Minigames(bot=None), ctx)
        assert '**0:44.75**' in ctx.sent[-1]['embed'].description

    def test_own_attachment_without_reply(self, monkeypatch):
        self._patch_analyze(monkeypatch)
        ctx = _make_ctx(attachments=[_VideoAttachment()])
        _run(Minigames(bot=None), ctx)
        assert '**0:44.75**' in ctx.sent[-1]['embed'].description

    def test_no_video_anywhere_errors(self, monkeypatch):
        self._patch_analyze(monkeypatch)
        ctx = _make_ctx(reply_to=_source_message([_FakeAttachment('a.png', b'x')]))
        with pytest.raises(MinigameCogError, match='screen recording'):
            _run(Minigames(bot=None), ctx)
        assert ctx.sent == []

    def test_oversized_video_rejected_before_download(self, monkeypatch):
        seen = {}
        self._patch_analyze(monkeypatch, seen=seen)
        att = _VideoAttachment(size=queenstime_module._QUEENS_VIDEO_MAX_BYTES + 1)
        ctx = _make_ctx(reply_to=_source_message([att]))
        with pytest.raises(MinigameCogError, match='limit'):
            _run(Minigames(bot=None), ctx)
        assert seen == {}

    def test_analysis_error_dict_is_reported(self, monkeypatch):
        self._patch_analyze(
            monkeypatch,
            result={'error': 'no finishing move found after last flip',
                    'seconds_at_last_flip': 31})
        ctx = _make_ctx(reply_to=_source_message([_VideoAttachment()]))
        with pytest.raises(MinigameCogError) as info:
            _run(Minigames(bot=None), ctx)
        assert 'no finishing move' in str(info.value)
        assert 'last seen at 31s' in str(info.value)

    @pytest.mark.parametrize('exc, needle', [
        (FileNotFoundError(2, 'No such file', 'ffprobe'), '`ffprobe` was not found'),
        (VideoDecodeError('ffmpeg failed (1)'), 'Could not decode'),
        (VideoDecodeTimeout('deadline'), 'Gave up'),
        (VideoTooLongError(19_000, 10_000), r'19,000 frames; the limit is 10,000'),
        (ValueError('boom'), 'unexpectedly'),
    ])
    def test_failures_become_user_errors(self, monkeypatch, exc, needle):
        self._patch_analyze(monkeypatch, exc=exc)
        ctx = _make_ctx(reply_to=_source_message([_VideoAttachment()]))
        with pytest.raises(MinigameCogError, match=needle):
            _run(Minigames(bot=None), ctx)

    def test_low_confidence_warnings(self, monkeypatch):
        result = dict(_OK_RESULT, anchor='assumed first flip = 0:01, tens digit INCONSISTENT',
                      finish_kind='last board change (no grey-out seen)', uncertainty_s=0.5)
        self._patch_analyze(monkeypatch, result=result)
        ctx = _make_ctx(reply_to=_source_message([_VideoAttachment()]))
        _run(Minigames(bot=None), ctx)
        desc = ctx.sent[-1]['embed'].description
        assert 'Lower confidence' in desc
        assert 'tens digit' in desc
        assert 'Reset greying out' in desc
        assert 'low frame rate' in desc
        assert ctx.sent[-1]['embed'].color == queenstime_module._AMBER

    def test_scratch_file_is_removed(self, monkeypatch):
        seen = {}
        self._patch_analyze(monkeypatch, seen=seen)
        ctx = _make_ctx(reply_to=_source_message([_VideoAttachment()]))
        _run(Minigames(bot=None), ctx)
        import os
        assert not os.path.exists(seen['path'])
        assert not os.path.exists(os.path.dirname(seen['path']))

    def test_busy_gate_announces_queueing(self, monkeypatch):
        self._patch_analyze(monkeypatch)
        cog = Minigames(bot=None)
        ctx = _make_ctx(reply_to=_source_message([_VideoAttachment()]))

        async def scenario():
            gate = cog._queens_video_gate()
            await gate.acquire()
            task = asyncio.ensure_future(Minigames.queens_time.__wrapped__(cog, ctx))
            await asyncio.sleep(0)
            assert ctx.sent and 'queued' in ctx.sent[0]['embed'].description
            gate.release()
            await task
        monkeypatch.setattr(
            queenstime_module.discord_common, 'embed_neutral',
            lambda desc, **kw: SimpleNamespace(description=desc))
        asyncio.run(scenario())
        assert '**0:44.75**' in ctx.sent[-1]['embed'].description


class TestFrameGuard:
    """``analyze`` refuses long clips before touching the decoder."""

    def _patch_probe(self, monkeypatch, frames):
        monkeypatch.setattr(queens_video_analyze, 'probe_frame_count',
                            lambda path, deadline=None: frames)

        def boom(*a, **k):
            raise AssertionError('decoder must not run')
        monkeypatch.setattr(queens_video_analyze, '_decode_with_timer', boom)

    def test_refuses_over_limit(self, monkeypatch):
        self._patch_probe(monkeypatch, 10_001)
        with pytest.raises(VideoTooLongError) as info:
            queens_video_analyze.analyze('x.mp4', max_frames=10_000)
        assert (info.value.frames, info.value.max_frames) == (10_001, 10_000)

    @pytest.mark.parametrize('frames', [10_000, None])
    def test_allows_at_limit_or_unknown(self, monkeypatch, frames):
        self._patch_probe(monkeypatch, frames)
        with pytest.raises(AssertionError, match='decoder must not run'):
            queens_video_analyze.analyze('x.mp4', max_frames=10_000)

    def test_no_limit_skips_probe(self, monkeypatch):
        def no_probe(*a, **k):
            raise AssertionError('probe must not run')
        monkeypatch.setattr(queens_video_analyze, 'probe_frame_count', no_probe)
        monkeypatch.setattr(queens_video_analyze, '_decode_with_timer',
                            lambda *a, **k: (_ for _ in ()).throw(RuntimeError('decoded')))
        with pytest.raises(RuntimeError, match='decoded'):
            queens_video_analyze.analyze('x.mp4')


class TestFindTool:
    """ffmpeg is located even when the bot service's PATH lacks /usr/bin."""

    def _fs(self, monkeypatch, present):
        monkeypatch.setattr(queens_video_decode.os.path, 'isfile', lambda p: p in present)
        monkeypatch.setattr(queens_video_decode.os, 'access', lambda p, mode: p in present)

    def test_empty_path_falls_back_to_usr_bin(self, monkeypatch):
        monkeypatch.setenv('PATH', '/opt/tle/.venv/bin')
        monkeypatch.delenv('FFMPEG_DIR', raising=False)
        monkeypatch.setattr(queens_video_decode.shutil, 'which', lambda n: None)
        self._fs(monkeypatch, {'/usr/bin/ffmpeg'})
        assert queens_video_decode.find_tool('ffmpeg') == '/usr/bin/ffmpeg'
        assert queens_video_decode.find_tool('ffprobe') is None

    def test_ffmpeg_dir_override_wins(self, monkeypatch):
        monkeypatch.setenv('FFMPEG_DIR', '/opt/ffmpeg')
        monkeypatch.setattr(queens_video_decode.shutil, 'which', lambda n: '/usr/bin/' + n)
        self._fs(monkeypatch, {'/opt/ffmpeg/ffmpeg', '/usr/bin/ffmpeg'})
        assert queens_video_decode.find_tool('ffmpeg') == '/opt/ffmpeg/ffmpeg'

    def test_path_hit_is_used(self, monkeypatch):
        monkeypatch.delenv('FFMPEG_DIR', raising=False)
        monkeypatch.setattr(queens_video_decode.shutil, 'which', lambda n: '/usr/local/bin/' + n)
        self._fs(monkeypatch, {'/usr/local/bin/ffprobe'})
        assert queens_video_decode.find_tool('ffprobe') == '/usr/local/bin/ffprobe'
