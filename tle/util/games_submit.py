"""Self-only submissions using the bot's existing parsers and rating pipeline."""
import asyncio
import datetime as dt
import time
from types import SimpleNamespace

import discord

from tle.util.complaints import ComplaintError


class GamesSubmissionService:
    def __init__(self, imports):
        self.imports = imports
        self.active = set()

    @property
    def db(self):
        return self.imports.db

    @property
    def cog(self):
        return self.imports.cog

    async def context(self, guild, member, name):
        if not isinstance(name, str):
            raise ComplaintError(400, 'game must be a game ID.')
        game = self.cog.GAMES.get(name)
        if game is None or not (game.linkedin_identity or name == 'akari'):
            raise ComplaintError(400, 'Unsupported personal game.')
        if self.db.get_guild_config(guild.id, game.feature_flag) != '1':
            raise ComplaintError(403, 'This game is not enabled in your server.')
        if self.cog._is_ingest_banned(guild.id, member.id, game):
            raise ComplaintError(403, 'You are banned from this game.')
        channel_id = self.db.get_minigame_channel(guild.id, name)
        channel = guild.get_channel_or_thread(int(channel_id)) if channel_id else None
        if channel is None:
            raise ComplaintError(409, f'Set an available game channel with ;{name} here first.')
        if isinstance(channel, discord.Thread) and channel.is_private():
            for who in (member, guild.me):
                if channel.permissions_for(who).manage_threads or channel.get_member(who.id):
                    continue
                try:
                    await asyncio.wait_for(channel.fetch_member(who.id), 5)
                except (discord.HTTPException, OSError, asyncio.TimeoutError):
                    raise ComplaintError(403, 'You and the bot must be members of the private game thread.') from None
        # Thread membership fetches may await IO; recheck mutable policies after them.
        if (self.db.get_guild_config(guild.id, game.feature_flag) != '1'
                or self.cog._is_ingest_banned(guild.id, member.id, game)
                or str(self.db.get_minigame_channel(guild.id, name)) != str(channel.id)):
            raise ComplaintError(403, 'Game access or channel configuration changed. Try again.')
        for who in (member, guild.me):
            permissions = channel.permissions_for(who)
            can_send = (permissions.send_messages_in_threads if isinstance(channel, discord.Thread)
                        else permissions.send_messages)
            if not permissions.view_channel or not can_send:
                raise ComplaintError(403, 'You and the bot must be able to view and post in the game channel.')
        if getattr(channel, 'archived', False) or getattr(channel, 'locked', False):
            raise ComplaintError(403, 'The game thread is archived or locked.')
        if game.linkedin_identity and not self.cog._queens_links_by_user(guild.id, game).get(str(member.id)):
            raise ComplaintError(409, 'Register your LinkedIn name in Discord with ;queens register NAME first.')
        return game, channel

    def parse(self, game, body):
        from tle.cogs._minigame_akari import expected_puzzle_number
        from tle.cogs._minigame_linkedin import linkedin_current_puzzle_date
        try:
            date = body['puzzle_date']
            if not isinstance(date, str) or len(date) != 10:
                raise ValueError()
            date = dt.date.fromisoformat(date)
        except ValueError:
            raise ComplaintError(400, 'puzzle_date must be YYYY-MM-DD.') from None
        number, seconds = body['puzzle_number'], body['time_seconds']
        accuracy, perfect = body['accuracy'], body['is_perfect']
        if type(number) is not int or number < 1 or type(seconds) is not int or not 0 <= seconds <= 86399:
            raise ComplaintError(400, 'Invalid puzzle number or solve time (0–86399 seconds).')
        if type(accuracy) is not int or not 0 <= accuracy <= 100 or type(perfect) is not bool:
            raise ComplaintError(400, 'accuracy must be 0–100 and is_perfect must be a boolean.')
        if perfect and accuracy != 100:
            raise ComplaintError(400, 'A perfect solve must have 100% accuracy.')
        time_text = (f'{seconds // 3600}:{seconds // 60 % 60:02}:{seconds % 60:02}'
                     if seconds >= 3600 else f'{seconds // 60}:{seconds % 60:02}')
        if game.linkedin_identity:
            today = linkedin_current_puzzle_date()
            expected = game.linkedin.number_for_date(date)
            # Personal LinkedIn results always follow the requested clean-You rule.
            if accuracy != 100 or not perfect:
                raise ComplaintError(400, 'Personal LinkedIn results must be marked clean.')
            content = f'{game.display_name.removeprefix("LinkedIn ")} #{number} | {time_text}'
        else:
            # Daily Akari rolls over in the player's local timezone (including UTC+14).
            today = dt.datetime.now(dt.timezone(dt.timedelta(hours=14))).date()
            expected = expected_puzzle_number(date)
            badge = '🌟 Perfect!' if perfect else f'🎯 {accuracy}%'
            content = f'Daily Akari 😊 {number}\n✅{date.isoformat()}✅\n{badge}  🕓 {time_text}'
        if number != expected or date > today:
            raise ComplaintError(400, 'Puzzle date and number do not match a published puzzle.')
        results = game.parse(content)
        if len(results) != 1:
            raise ComplaintError(400, 'The game parser rejected this result.')
        parsed = results[0]
        if (parsed.accuracy, parsed.time_seconds, parsed.is_perfect) != (accuracy, seconds, perfect):
            raise ComplaintError(400, 'The game parser could not preserve the submitted score.')
        return content, parsed

    def _existing(self, guild, member, game, parsed):
        existing = self.db.get_minigame_result_for_user_puzzle(
            guild.id, game.name, member.id, parsed.puzzle_number)
        if game.linkedin_identity:
            link = self.cog._queens_links_by_user(guild.id, game).get(str(member.id))
            date = game.linkedin.date_for_number(parsed.puzzle_number)
            sources = [row for number in game.linkedin.puzzle_numbers_for_date(date)
                       for row in self.db.get_minigame_unresolved_results_for_puzzle(guild.id, game.name, number)
                       if link and row.normalized_name == link.normalized_name]
            if sources:
                existing = max(sources, key=lambda row: (
                    not bool(row.is_rated), row.puzzle_number == parsed.puzzle_number, row.stored_at))
        if existing and (existing.time_seconds, existing.accuracy, bool(existing.is_perfect)) != (
                parsed.time_seconds, parsed.accuracy, parsed.is_perfect):
            raise ComplaintError(409, 'A different result is already registered for you. Ask a moderator to correct it.')
        return existing

    async def submit(self, guild, member, body, *, reauthenticate):
        key = (str(guild.id), str(member.id))
        if key in self.active or len(self.active) >= 16:
            raise ComplaintError(409, 'A submission is in progress. Retry shortly.')
        self.active.add(key)
        try:
            await reauthenticate()
            game, channel = await self.context(guild, member, body['game'])
            content, parsed = self.parse(game, body)
            row = self.db.get_games_submission(guild.id, member.id, game.name, parsed.puzzle_number)
            if row is not None and row.content != content:
                raise ComplaintError(409, 'This puzzle already has a different personal submission.')
            if row is not None and row.completed:
                if row.completed < 0 or self._existing(guild, member, game, parsed) is None:
                    raise ComplaintError(409, 'Your previous post or registered score was removed. Ask a moderator to restore it.')
                return self.receipt(row, duplicate=True)
            self._existing(guild, member, game, parsed)
            fresh = row is None
            row = row or self.db.claim_games_submission(
                guild.id, member.id, game.name, parsed.puzzle_number, channel.id, content)
            if str(channel.id) != row.channel_id:
                raise ComplaintError(409, 'The game channel changed during submission. Ask a moderator to reconcile the pending post.')
            if row.message_id is None:
                message_id = await self._post(row, channel, guild, member, game, fresh, reauthenticate)
                self.db.mark_games_submission_posted(row.nonce, message_id)
                row = self.db.get_games_submission(guild.id, member.id, game.name, parsed.puzzle_number)
            self._existing(guild, member, game, parsed)
            # Save with the token owner's identity; bot-authored posts are ignored by on_message.
            message = SimpleNamespace(id=int(row.message_id), guild=guild, author=member,
                                      channel=channel, content=content,
                                      created_at=dt.datetime.fromtimestamp(row.created_at, dt.timezone.utc))
            self.db.save_raw_message(message.id, guild.id, channel.id, member.id,
                                     message.created_at.isoformat(), content)
            await self.cog._ingest_message(message, game)
            self.cog._recompute_game_ratings(guild.id, game)
            if self._existing(guild, member, game, parsed) is None:
                raise ComplaintError(503, 'Your score was posted but could not be registered. Retry to recover it.')
            self.db.finish_games_submission(row.nonce)
            return self.receipt(row, duplicate=False)
        finally:
            self.active.discard(key)

    async def _post(self, row, channel, guild, member, game, fresh, reauthenticate):
        # After a crash/ambiguous timeout, recover the original message by its
        # Discord nonce. Never blindly repost outside Discord's dedup window.
        if not fresh:
            try:
                after = dt.datetime.fromtimestamp(row.created_at - 1, dt.timezone.utc)
                async for message in channel.history(limit=100, after=after, oldest_first=True):
                    if str(message.author.id) == str(guild.me.id) and str(message.nonce) == row.nonce:
                        return message.id
            except (discord.HTTPException, OSError):
                pass
            if time.time() - row.created_at > 120:
                raise ComplaintError(409, 'Discord delivery could not be verified. Ask a moderator to check the pending post; it will not be duplicated.')
        await reauthenticate()
        member = guild.get_member(member.id) or member
        _, current_channel = await self.context(guild, member, game.name)
        if current_channel.id != channel.id:
            raise ComplaintError(409, 'The game channel changed. Retry after checking your setup.')
        await reauthenticate()
        display = member.display_name
        if game.linkedin_identity:
            display = self.cog._queens_public_user_name(
                guild, member.id, self.cog._queens_links_by_user(guild.id, game))
        content = f'{discord.utils.escape_markdown(display)}\n{row.content}'
        try:
            message = await asyncio.wait_for(channel.send(
                content, allowed_mentions=discord.AllowedMentions.none(), nonce=row.nonce), 20)
        except discord.HTTPException as exc:
            if 400 <= exc.status < 500:
                self.db.release_games_submission(row.nonce)
            raise ComplaintError(503, 'Discord could not post your score. Retry shortly.') from None
        except (OSError, asyncio.TimeoutError):
            raise ComplaintError(503, 'Discord delivery is uncertain. Retry shortly to recover the same post.') from None
        return message.id

    @staticmethod
    def receipt(row, *, duplicate):
        return {'game': row.game, 'puzzle_number': row.puzzle_number, 'duplicate': duplicate,
                'message_url': f'https://discord.com/channels/{row.guild_id}/{row.channel_id}/{row.message_id}',
                'registered': True, 'posted': True}
