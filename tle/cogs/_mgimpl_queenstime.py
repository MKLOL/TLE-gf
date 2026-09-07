"""``;queens time``: read a solve time off a replied-to screen recording.

Minigames cog impl mixin (see ``minigames.py``).  The heavy lifting lives in
``tle.util.queens_video_analyze``; this module only picks the video attachment
off the replied-to (or invoking) message, downloads it to a scratch file, runs
the analysis in a worker thread under a bot-wide semaphore, and renders the
result as an embed.
"""
import asyncio
import logging
import os
import shutil
import tempfile
import time

import discord

from tle.util import discord_common
from tle.cogs._minigame_helpers import MinigameCogError

logger = logging.getLogger(__name__)

_GREEN = 0x28A745
_AMBER = 0xFFBF00

# Discord caps uploads at 10 MiB without Nitro and 500 MiB with; a 5-minute
# phone screen recording is ~70 MiB.  Anything larger is not a solve clip.
_QUEENS_VIDEO_MAX_BYTES = 256 * 1024 * 1024
# End-to-end analysis budget.  A 5-minute clip analyses in ~6 s on a laptop;
# the bot host is slower, and ffmpeg decoding dominates, so leave headroom.
_QUEENS_VIDEO_TIMEOUT_SECONDS = 180
# Peak RAM scales with the frame count, not the file size: the full-resolution
# timer strip of every frame is held at once (~280 MB for 7.7k frames at
# 880x1920, ~1 GB for 19k frames at 1080p/60 fps).  Refuse clips beyond this
# before decoding anything; 10k frames is ~5.5 min at 30 fps or ~2.8 min at 60.
_QUEENS_VIDEO_MAX_FRAMES = 10_000
# Only one recording is decoded at a time: ffmpeg + numpy saturate every core.
_QUEENS_VIDEO_MAX_CONCURRENT = 1
_VIDEO_EXTENSIONS = ('.mp4', '.mov', '.m4v', '.webm', '.mkv', '.avi', '.3gp')


def _is_video_attachment(attachment):
    content_type = (getattr(attachment, 'content_type', None) or '').lower()
    if content_type.startswith('video/'):
        return True
    filename = (getattr(attachment, 'filename', None) or '').lower()
    return filename.endswith(_VIDEO_EXTENSIONS)


def pick_video_attachment(message):
    """First video attachment on ``message`` or ``None``."""
    for attachment in list(getattr(message, 'attachments', None) or []):
        if _is_video_attachment(attachment):
            return attachment
    return None


def _missing_ffmpeg_tool():
    """Name of the first of ffmpeg/ffprobe the decoder cannot locate, or None."""
    from tle.util.queens_video_decode import find_tool
    for tool in ('ffmpeg', 'ffprobe'):
        if find_tool(tool) is None:
            return tool
    return None


def _ffmpeg_missing_error(tool):
    # The PATH goes to the log only; it is for the operator, not the channel.
    logger.error('queens time: %s not found on PATH=%r, in the usual directories, or via '
                 'static-ffmpeg (see the warning above); install ffmpeg or set FFMPEG_DIR',
                 tool, os.environ.get('PATH'))
    return MinigameCogError(
        f'Video analysis is unavailable: `{tool}` was not found by the bot process and the '
        f'bundled build could not be fetched. See the bot log; install ffmpeg on the bot '
        f'host or point `FFMPEG_DIR` at its directory.')


def _safe_video_filename(attachment):
    base = os.path.basename(getattr(attachment, 'filename', None) or 'video')
    root, ext = os.path.splitext(base)
    ext = ext.lower() if ext.lower() in _VIDEO_EXTENSIONS else '.mp4'
    root = ''.join(ch if ch.isalnum() or ch in '-_.' else '_' for ch in root) or 'video'
    return f'{root[:80]}{ext}'


def format_queens_time_embed(game, result, source_message=None):
    """Build the reply embed for a successful ``analyze`` result."""
    from tle.util.queens_video_analyze import format_solve_time
    time_text = format_solve_time(result['time_s'])
    uncertainty = float(result.get('uncertainty_s') or 0.0)
    lines = [f'**{time_text}**  (±{uncertainty:.2f} s)']
    shown = result.get('seconds_shown')
    if shown is not None:
        lines.append(f'Timer shown on screen: `{format_solve_time(int(shown), decimals=0)}`')
    if source_message is not None:
        author = getattr(source_message, 'author', None)
        mention = getattr(author, 'mention', None)
        if mention:
            lines.append(f'Recording by {mention}')
    anchor = str(result.get('anchor') or '')
    warnings = []
    if 'INCONSISTENT' in anchor:
        warnings.append('the tens digit did not confirm the second count')
    if 'no tens roll' in anchor and result.get('ocr_seconds') is None:
        warnings.append('the digits could not be read, so the whole seconds are inferred')
    if 'no grey-out seen' in str(result.get('finish_kind') or ''):
        warnings.append('the solve confirmation (Reset greying out) was not seen')
    if uncertainty > 0.25:
        warnings.append('the recording has a low frame rate')
    if warnings:
        lines.append('⚠️ Lower confidence: ' + '; '.join(warnings) + '.')
    embed = discord.Embed(
        title=f'{game.display_name} solve time',
        description='\n'.join(lines),
        color=_AMBER if warnings else _GREEN,
    )
    footer = (f"{result.get('flips', 0)} timer flips · "
              f"frame interval {result.get('frame_interval_ms', 0):.0f} ms · "
              f"{result.get('finish_kind', '')}")
    embed.set_footer(text=footer)
    return embed


class ImplQueensTimeMixin:
    def _queens_video_gate(self):
        """Bot-wide gate (one cog instance) created lazily on the running loop."""
        gate = getattr(self, '_queens_video_semaphore', None)
        if gate is None:
            gate = asyncio.Semaphore(_QUEENS_VIDEO_MAX_CONCURRENT)
            self._queens_video_semaphore = gate
        return gate

    async def _queens_time_source_message(self, ctx):
        """The replied-to message if the command is a reply, else the command itself."""
        message = ctx.message
        reference = getattr(message, 'reference', None)
        message_id = getattr(reference, 'message_id', None) if reference else None
        if message_id is None:
            return message
        resolved = getattr(reference, 'resolved', None)
        if resolved is not None and hasattr(resolved, 'attachments'):
            return resolved
        try:
            return await ctx.channel.fetch_message(message_id)
        except Exception as exc:  # NotFound / Forbidden / HTTPException
            raise MinigameCogError(
                f'Could not fetch the replied-to message ({exc}).') from exc

    async def _cmd_queens_time(self, ctx, game):
        source = await self._queens_time_source_message(ctx)
        attachment = pick_video_attachment(source)
        if attachment is None:
            raise MinigameCogError(
                f'Reply to a message containing a screen recording of a '
                f'{game.display_name} solve (or attach one) with '
                f'`;{game.name} time`.')
        size = int(getattr(attachment, 'size', 0) or 0)
        if size > _QUEENS_VIDEO_MAX_BYTES:
            raise MinigameCogError(
                f'That video is {size / (1024 * 1024):.0f} MiB; the limit is '
                f'{_QUEENS_VIDEO_MAX_BYTES // (1024 * 1024)} MiB.')
        gate = self._queens_video_gate()
        if gate.locked():
            await ctx.send(embed=discord_common.embed_neutral(
                'Another recording is being analysed; yours is queued.'))
        async with gate:
            # Resolving the tools may fetch the bundled ffmpeg on first use (tens of
            # MB), so it runs off the loop like the analysis itself.
            missing = await asyncio.get_running_loop().run_in_executor(
                None, _missing_ffmpeg_tool)
            if missing is not None:
                raise _ffmpeg_missing_error(missing)
            result = await self._queens_time_analyze_attachment(ctx, attachment)
        if 'error' in result:
            extra = ''
            if 'seconds_at_last_flip' in result:
                extra = f" (timer last seen at {result['seconds_at_last_flip']}s)"
            raise MinigameCogError(
                f"Could not read the solve time: {result['error']}{extra}.")
        await ctx.send(embed=format_queens_time_embed(game, result, source))

    async def _queens_time_analyze_attachment(self, ctx, attachment):
        tmpdir = tempfile.mkdtemp(prefix='queens-time-')
        try:
            path = os.path.join(tmpdir, _safe_video_filename(attachment))
            with open(path, 'wb') as fh:
                fh.write(await attachment.read())
            typing = getattr(ctx, 'typing', None)
            if typing is None:
                return await self._queens_time_run(path)
            async with typing():
                return await self._queens_time_run(path)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    async def _queens_time_run(self, path):
        from tle.util.queens_video_analyze import analyze
        from tle.util.queens_video_decode import (
            VideoDecodeError, VideoDecodeTimeout, VideoTooLongError,
        )
        # The deadline is checked inside the decoder so ffmpeg is actually killed;
        # ``wait_for`` is only a backstop should the worker thread wedge.
        deadline = time.monotonic() + _QUEENS_VIDEO_TIMEOUT_SECONDS
        try:
            loop = asyncio.get_running_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(None, lambda: analyze(
                    path, deadline=deadline, max_frames=_QUEENS_VIDEO_MAX_FRAMES)),
                timeout=_QUEENS_VIDEO_TIMEOUT_SECONDS + 15)
        except (asyncio.TimeoutError, VideoDecodeTimeout):
            raise MinigameCogError(
                f'Gave up after {_QUEENS_VIDEO_TIMEOUT_SECONDS} s; the recording is '
                f'too long to analyse.')
        except VideoTooLongError as exc:
            raise MinigameCogError(
                f'That recording has about {exc.frames:,} frames; the limit is '
                f'{exc.max_frames:,} (roughly {exc.max_frames // 1800} minutes at 30 fps). '
                f'Trim it to the solve, or record at a lower frame rate.')
        except FileNotFoundError as exc:
            raise _ffmpeg_missing_error(exc.filename or 'ffmpeg')
        except VideoDecodeError as exc:
            raise MinigameCogError(f'Could not decode that video: {exc}.')
        except Exception:
            logger.exception('queens time: analysis failed for %s', os.path.basename(path))
            raise MinigameCogError('Video analysis failed unexpectedly; see the bot log.')
