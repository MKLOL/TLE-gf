"""Chronological recovery of historical Great Day membership events."""
from __future__ import annotations

from dataclasses import dataclass, field

from tle.cogs._greatday_event_channels import merged_channel_history
from tle.cogs._greatday_event_parse import (
    MembershipCommand,
    MembershipResult,
    compatible as _compatible,
    message_time as _message_time,
    parse_membership_command,
    parse_membership_result,
    resolve_target as _resolve_target,
)


_REPLY_WINDOW_SECONDS = 15.0
_SIGNUP_COMMANDS = frozenset(('signup', 'add'))
_SIGNOUT_COMMANDS = frozenset(('remove', 'kick'))


@dataclass
class SignupScanAudit:
    channels_requested: int = 0
    unreadable_channels: int = 0
    discovery_failures: int = 0
    scanned: int = 0
    commands: int = 0
    bot_results: int = 0
    matched_successes: int = 0
    matched_failures: int = 0
    commands_without_result: int = 0
    unmatched_results: int = 0
    ambiguous_matches: int = 0
    unresolved_targets: int = 0
    inferred_targets: int = 0
    unknown_ban_states: int = 0
    state_conflicts: int = 0
    membership_mismatches: int = 0
    ban_mismatches: int = 0
    uncertain_user_ids: set[str] = field(default_factory=set, repr=False)

    @property
    def trustworthy(self):
        """Whether the scan found no evidence that inferred events are wrong."""
        return not any((
            self.unreadable_channels,
            self.discovery_failures,
            self.unmatched_results,
            self.ambiguous_matches,
            self.unresolved_targets,
            self.inferred_targets,
            self.unknown_ban_states,
            self.state_conflicts,
            self.membership_mismatches,
            self.ban_mismatches,
        ))

    def summary(self):
        """Return compact lines suitable for a Discord embed."""
        lines = [
            f'Channels: **{self.channels_requested}**; unreadable: '
            f'**{self.unreadable_channels}**; thread listing failures: '
            f'**{self.discovery_failures}**',
            f'Commands: **{self.commands}**; bot results: '
            f'**{self.bot_results}**',
            f'Matched successes: **{self.matched_successes}**; '
            f'rejections: **{self.matched_failures}**',
            f'Assumed no result: **{self.commands_without_result}**; '
            f'unmatched bot results: **{self.unmatched_results}**',
        ]
        warnings = [
            ('ambiguous', self.ambiguous_matches),
            ('name-resolved', self.inferred_targets),
            ('unresolved targets', self.unresolved_targets),
            ('unknown-state bans', self.unknown_ban_states),
            ('state conflicts', self.state_conflicts),
            ('membership mismatches', self.membership_mismatches),
            ('ban mismatches', self.ban_mismatches),
        ]
        shown = [f'{label}: **{count}**' for label, count in warnings if count]
        lines.append('Warnings: ' + ('; '.join(shown) if shown else '**none**'))
        return '\n'.join(lines)


@dataclass(frozen=True)
class SignupScanResult:
    scanned: int
    events: list[tuple]
    audit: SignupScanAudit
    active_user_ids: frozenset[str]
    banned_user_ids: frozenset[str]


def _pick_command(pending, result, audit, window_seconds):
    candidates = [
        command for command in pending
        if _compatible(command, result)
        and 0 <= result.at - command.at <= window_seconds
    ]
    if result.reference_id is not None:
        referenced = [command for command in candidates
                      if command.message_id == result.reference_id]
        if referenced:
            candidates = referenced
    if not candidates:
        return None
    if len(candidates) > 1:
        audit.ambiguous_matches += 1
        audit.uncertain_user_ids.update(
            command.target_id for command in candidates if command.target_id)
    return candidates[-1]


def _append_event(events, guild_id, user_id, action, command):
    events.append((guild_id, user_id, action, command.at, command.message_id))


def _state_conflict(audit, user_id):
    audit.state_conflicts += 1
    audit.uncertain_user_ids.add(user_id)


def _apply_success(command, user_id, guild_id, states, banned, events, audit):
    known = user_id in states
    active = states.get(user_id, False)
    if command.kind in _SIGNUP_COMMANDS:
        if known and active:
            _state_conflict(audit, user_id)
        if banned.get(user_id, False):
            _state_conflict(audit, user_id)
        _append_event(events, guild_id, user_id, 'signup', command)
        states[user_id] = True
        banned[user_id] = False
    elif command.kind in _SIGNOUT_COMMANDS:
        if not active:
            _state_conflict(audit, user_id)
        _append_event(events, guild_id, user_id, 'signout', command)
        states[user_id] = False
    elif command.kind == 'ban':
        if banned.get(user_id, False):
            _state_conflict(audit, user_id)
        if active:
            _append_event(events, guild_id, user_id, 'signout', command)
        elif not known:
            audit.unknown_ban_states += 1
            audit.uncertain_user_ids.add(user_id)
        states[user_id] = False
        banned[user_id] = True
    elif command.kind == 'unban':
        if not banned.get(user_id, False):
            _state_conflict(audit, user_id)
        banned[user_id] = False


def _apply_failure(result, user_id, states, banned, audit):
    active = states.get(user_id, False)
    if result.outcome == 'already_signed':
        if not active:
            _state_conflict(audit, user_id)
        states[user_id] = True
        banned[user_id] = False
    elif result.outcome in ('banned', 'already_banned'):
        if active or not banned.get(user_id, False):
            _state_conflict(audit, user_id)
        states[user_id] = False
        banned[user_id] = True
    elif result.outcome == 'not_signed':
        if active:
            _state_conflict(audit, user_id)
        states[user_id] = False
    elif result.outcome == 'not_banned':
        if banned.get(user_id, False):
            _state_conflict(audit, user_id)
        banned[user_id] = False


def _expire_pending(pending, now, audit, window_seconds):
    kept = []
    for command in pending:
        if now - command.at > window_seconds:
            audit.commands_without_result += 1
        else:
            kept.append(command)
    return kept


def _audit_final_state(states, banned, current_signups, current_bans, audit):
    if current_signups is not None:
        replayed = {user_id for user_id, active in states.items() if active}
        mismatched = replayed.symmetric_difference(set(current_signups))
        audit.membership_mismatches = len(mismatched)
        audit.uncertain_user_ids.update(mismatched)
    if current_bans is not None:
        replayed = {user_id for user_id, active in banned.items() if active}
        mismatched = replayed.symmetric_difference(set(current_bans))
        audit.ban_mismatches = len(mismatched)
        audit.uncertain_user_ids.update(mismatched)


async def scan_signup_events_audited(
        channel, guild_id, bot_user_id, *, guild=None,
        current_signup_ids=None, current_ban_ids=None, progress=None,
        progress_interval=250, window_seconds=_REPLY_WINDOW_SECONDS):
    """Match commands to exact bot results, then replay their state changes."""
    audit = SignupScanAudit(channels_requested=1)
    history = merged_channel_history((channel,))
    return await _scan_signup_history(
        history, guild_id, bot_user_id, guild=guild,
        current_signup_ids=current_signup_ids,
        current_ban_ids=current_ban_ids, progress=progress,
        progress_interval=progress_interval, window_seconds=window_seconds,
        audit=audit)


async def scan_signup_events_channels_audited(
        channels, guild_id, bot_user_id, *, guild=None,
        current_signup_ids=None, current_ban_ids=None, progress=None,
        progress_interval=250, window_seconds=_REPLY_WINDOW_SECONDS,
        discovery_failures=0):
    """Scan all supplied channels as one chronological guild history."""
    channels = tuple(channels)
    audit = SignupScanAudit(
        channels_requested=len(channels),
        discovery_failures=discovery_failures,
    )
    unreadable = set()

    def on_unreadable(channel_key):
        unreadable.add(channel_key)
        audit.unreadable_channels = len(unreadable)

    history = merged_channel_history(
        channels, tolerate_unreadable=True, on_unreadable=on_unreadable)
    return await _scan_signup_history(
        history, guild_id, bot_user_id, guild=guild,
        current_signup_ids=current_signup_ids,
        current_ban_ids=current_ban_ids, progress=progress,
        progress_interval=progress_interval, window_seconds=window_seconds,
        audit=audit)


async def _scan_signup_history(
        history, guild_id, bot_user_id, *, guild, current_signup_ids,
        current_ban_ids, progress, progress_interval, window_seconds, audit):
    """Match and replay one globally ordered, channel-keyed message stream."""
    events = []
    pending_by_channel = {}
    states = {}
    banned = {}
    async for channel_key, message in history:
        audit.scanned += 1
        now = _message_time(message)
        pending = _expire_pending(
            pending_by_channel.get(channel_key, []), now, audit,
            window_seconds)
        pending_by_channel[channel_key] = pending
        result = parse_membership_result(message, bot_user_id)
        if result is not None:
            audit.bot_results += 1
            command = _pick_command(pending, result, audit, window_seconds)
            if command is None:
                audit.unmatched_results += 1
            else:
                pending.remove(command)
                user_id, inferred = _resolve_target(command, result, guild)
                if result.success:
                    audit.matched_successes += 1
                else:
                    audit.matched_failures += 1
                if user_id is None:
                    audit.unresolved_targets += 1
                else:
                    if inferred:
                        audit.inferred_targets += 1
                        audit.uncertain_user_ids.add(user_id)
                    if result.success:
                        _apply_success(
                            command, user_id, str(guild_id), states, banned,
                            events, audit)
                    else:
                        _apply_failure(result, user_id, states, banned, audit)
        else:
            command = parse_membership_command(message, bot_user_id)
            if command is not None:
                audit.commands += 1
                pending.append(command)
        if progress is not None and audit.scanned % progress_interval == 0:
            await progress(audit.scanned, audit.matched_successes)
    audit.commands_without_result += sum(
        len(pending) for pending in pending_by_channel.values())
    _audit_final_state(
        states, banned, current_signup_ids, current_ban_ids, audit)
    return SignupScanResult(
        audit.scanned, events, audit,
        frozenset(user_id for user_id, active in states.items() if active),
        frozenset(user_id for user_id, active in banned.items() if active))


async def scan_signup_events(channel, guild_id, bot_user_id, progress=None,
                             progress_interval=250):
    """Compatibility wrapper returning the original ``(scanned, events)``."""
    result = await scan_signup_events_audited(
        channel, guild_id, bot_user_id, progress=progress,
        progress_interval=progress_interval)
    return result.scanned, result.events
