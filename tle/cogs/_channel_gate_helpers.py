"""Pure helpers for the channel-gate cog (``channel_gate.py``).

Kept discord-free so the gate decision can be unit-tested without the bot.
"""
from discord.ext import commands


class ChannelGateError(commands.CommandError):
    """User-facing error for the ``;disallow`` / ``;allow`` commands."""
    pass


def gate_decision(gate, current_thread_id):
    """Decide whether a command may run, given the ``command_gate`` row (or
    None) for the invocation's parent channel and the id of the thread it was
    run in (or None if run in the channel itself).

    Returns ``(allowed, allowed_thread_id)``. ``allowed_thread_id`` is the
    bot-created command thread (may be None) — used only to link the user to a
    place where commands work; the command need NOT have been run in it.

    Rules:
    - No gate -> allowed.
    - ``;disallow`` (``thread_id`` is None): the channel and *all* of its
      threads are blocked.
    - ``;disallow thread`` (``thread_id`` set): the channel's main timeline is
      blocked, but commands are allowed in *any* thread under it — not just the
      bot-created one.
    """
    if gate is None:
        return True, None
    allowed_thread_id = gate.thread_id
    in_thread = current_thread_id is not None
    thread_mode = allowed_thread_id is not None
    if in_thread and thread_mode:
        return True, allowed_thread_id
    return False, allowed_thread_id
