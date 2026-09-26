"""Tests for codeforces_common.resolve_handles and _resolve_member_by_name."""
import asyncio
import sys

import pytest

cf_common = sys.modules['tle.util.codeforces_common']
commands = sys.modules['discord.ext.commands']


# ── Fakes ──────────────────────────────────────────────────────────────────

class FakeMember:
    def __init__(self, id, name, display_name=None):
        self.id = id
        self.name = name
        self.display_name = display_name or name
        self.mention = f'<@{id}>'

    def __str__(self):
        return self.name


class FakeGuild:
    def __init__(self, members=None):
        self.id = 999
        self.members = members or []
        self._member_map = {m.id: m for m in self.members}

    def get_member(self, member_id):
        return self._member_map.get(member_id)


class FakeCtx:
    def __init__(self, author, guild):
        self.author = author
        self.guild = guild


class FakeConverter:
    """MemberConverter stub — looks up by name in guild.members."""
    async def convert(self, ctx, argument):
        lowered = argument.lower()
        for m in ctx.guild.members:
            if m.name.lower() == lowered or m.display_name.lower() == lowered:
                return m
        raise commands.BadArgument(f'Member "{argument}" not found.')


class FakeUserDb:
    def __init__(self, handle_map=None, cached=()):
        # {(user_id, guild_id): cf_handle}
        self._handles = handle_map or {}
        # Handles the CF user cache knows, in Codeforces' own casing.
        self._cached = {h.lower(): h for h in cached}

    def get_handle(self, user_id, guild_id):
        return self._handles.get((user_id, str(guild_id)))

    def fetch_cf_user(self, handle):
        canonical = self._cached.get(handle.lower())
        if canonical is None:
            return None
        return type('User', (), {'handle': canonical})()

    def get_handles_for_guild(self, guild_id):
        return [(uid, h) for (uid, gid), h in self._handles.items()
                if gid == str(guild_id)]


def run(coro):
    return asyncio.run(coro)


# ── Fixtures ───────────────────────────────────────────────────────────────

ALICE = FakeMember(100, 'alice', display_name='Alice Nice')
BOB = FakeMember(200, 'bob', display_name='Bobby')
# EVE has the same display_name as Alice's username — tests ambiguity
EVE = FakeMember(300, 'eve', display_name='alice')


@pytest.fixture(autouse=True)
def patch_user_db(monkeypatch):
    """Inject a FakeUserDb into cf_common.user_db for every test."""
    db = FakeUserDb({
        (100, '999'): 'alice_cf',
        (200, '999'): 'bob_cf',
        (300, '999'): 'eve_cf',
    })
    monkeypatch.setattr(cf_common, 'user_db', db)
    return db


def _make_ctx(author=None, members=None):
    members = members if members is not None else [ALICE, BOB, EVE]
    author = author or ALICE
    return FakeCtx(author, FakeGuild(members))


CONVERTER = FakeConverter()


# ── Tests: self-lookup by ID (!<digits>) ──────────────────────────────────

class TestSelfLookupById:
    def test_self_lookup_returns_own_handle(self):
        ctx = _make_ctx(author=ALICE)
        handles = ('!' + str(ALICE.id),)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, handles))
        assert result == ['alice_cf']

    def test_self_lookup_not_found_raises(self):
        ctx = _make_ctx(author=ALICE)
        handles = ('!999999',)
        with pytest.raises(cf_common.FindMemberFailedError):
            run(cf_common.resolve_handles(ctx, CONVERTER, handles))

    def test_self_lookup_no_handle_registered_raises(self, patch_user_db):
        unregistered = FakeMember(400, 'nobody')
        ctx = _make_ctx(author=unregistered, members=[ALICE, BOB, EVE, unregistered])
        handles = ('!' + str(unregistered.id),)
        with pytest.raises(cf_common.HandleNotRegisteredError):
            run(cf_common.resolve_handles(ctx, CONVERTER, handles))


# ── Tests: Discord mention (<@id>) ────────────────────────────────────────

class TestMentionResolution:
    def test_mention_resolves_to_handle(self):
        ctx = _make_ctx()
        handles = (f'<@{BOB.id}>',)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, handles))
        assert result == ['bob_cf']

    def test_mention_with_nick_prefix(self):
        """Discord sometimes sends <@!id> for nickname mentions."""
        ctx = _make_ctx()
        handles = (f'<@!{BOB.id}>',)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, handles))
        assert result == ['bob_cf']

    def test_mention_unknown_member_raises(self):
        ctx = _make_ctx()
        handles = ('<@999999>',)
        with pytest.raises(cf_common.FindMemberFailedError):
            run(cf_common.resolve_handles(ctx, CONVERTER, handles))

    def test_mention_no_handle_raises(self, patch_user_db):
        unregistered = FakeMember(400, 'nobody')
        ctx = _make_ctx(members=[ALICE, BOB, EVE, unregistered])
        handles = (f'<@{unregistered.id}>',)
        with pytest.raises(cf_common.HandleNotRegisteredError):
            run(cf_common.resolve_handles(ctx, CONVERTER, handles))


# ── Tests: -c prefix (force CF handle) ────────────────────────────────────

class TestForceCfHandle:
    def test_dash_c_bypasses_discord_lookup(self):
        ctx = _make_ctx()
        # 'alice' matches a Discord member, but -c should skip that
        handles = ('-calice',)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, handles))
        assert result == ['alice']  # raw CF handle, not alice_cf

    def test_dash_c_with_tourist(self):
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('-ctourist',)))
        assert result == ['tourist']


# ── Tests: plain text (username → display name → CF handle) ───────────────

class TestPlainTextResolution:
    def test_matches_discord_username(self):
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('bob',)))
        assert result == ['bob_cf']

    def test_case_insensitive_username(self):
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('BOB',)))
        assert result == ['bob_cf']

    def test_matches_display_name_when_no_username_match(self):
        ctx = _make_ctx()
        # 'Bobby' is BOB's display_name, doesn't match any username
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('Bobby',)))
        assert result == ['bob_cf']

    def test_username_takes_priority_over_display_name(self):
        """EVE has display_name='alice', but ALICE has username='alice'.
        Searching for 'alice' should find ALICE (by username), not EVE (by display_name)."""
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('alice',)))
        assert result == ['alice_cf']

    def test_falls_through_to_cf_handle_when_no_member_found(self):
        ctx = _make_ctx()
        # 'tourist' matches no Discord member
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('tourist',)))
        assert result == ['tourist']

    def test_falls_through_when_member_found_but_no_cf_handle(self, monkeypatch):
        """If Discord member is found but has no registered CF handle,
        fall through to treating the text as a raw CF handle."""
        db = FakeUserDb({
            # Only ALICE registered, BOB is not
            (100, '999'): 'alice_cf',
        })
        monkeypatch.setattr(cf_common, 'user_db', db)
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('bob',)))
        # 'bob' matched Discord member BOB, but BOB has no CF handle,
        # so it falls through to raw CF handle 'bob'
        assert result == ['bob']


# ── Tests: !<name> backward compat ────────────────────────────────────────

class TestBangNameBackwardCompat:
    def test_bang_name_uses_converter(self):
        """Users can still type !username for versus command."""
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('!bob',)))
        assert result == ['bob_cf']

    def test_bang_name_with_hash_zero_suffix(self):
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('!bob#0',)))
        assert result == ['bob_cf']

    def test_bang_name_not_found_raises(self):
        ctx = _make_ctx()
        with pytest.raises(cf_common.FindMemberFailedError):
            run(cf_common.resolve_handles(ctx, CONVERTER, ('!nonexistent',)))


# ── Tests: _resolve_member_by_name ────────────────────────────────────────

class TestResolveMemberByName:
    def test_finds_by_username(self):
        guild = FakeGuild([ALICE, BOB, EVE])
        assert cf_common._resolve_member_by_name(guild, 'alice') is ALICE

    def test_finds_by_display_name(self):
        guild = FakeGuild([ALICE, BOB, EVE])
        assert cf_common._resolve_member_by_name(guild, 'Bobby') is BOB

    def test_username_beats_display_name(self):
        """EVE has display_name='alice', but ALICE has username='alice'."""
        guild = FakeGuild([ALICE, BOB, EVE])
        assert cf_common._resolve_member_by_name(guild, 'alice') is ALICE

    def test_display_name_when_no_username(self):
        guild = FakeGuild([ALICE, BOB, EVE])
        # 'Alice Nice' is ALICE's display_name, no username match
        assert cf_common._resolve_member_by_name(guild, 'Alice Nice') is ALICE

    def test_returns_none_when_not_found(self):
        guild = FakeGuild([ALICE, BOB, EVE])
        assert cf_common._resolve_member_by_name(guild, 'nobody') is None

    def test_case_insensitive(self):
        guild = FakeGuild([ALICE, BOB, EVE])
        assert cf_common._resolve_member_by_name(guild, 'ALICE') is ALICE
        assert cf_common._resolve_member_by_name(guild, 'bobby') is BOB


# ── Tests: handle count bounds ────────────────────────────────────────────

class TestHandleCountBounds:
    def test_too_few_raises(self):
        ctx = _make_ctx()
        with pytest.raises(cf_common.HandleCountOutOfBoundsError):
            run(cf_common.resolve_handles(ctx, CONVERTER, (), mincnt=1))

    def test_too_many_raises(self):
        ctx = _make_ctx()
        handles = ('a', 'b', 'c')
        with pytest.raises(cf_common.HandleCountOutOfBoundsError):
            run(cf_common.resolve_handles(ctx, CONVERTER, handles, maxcnt=2))


# ── Tests: vjudge rejection ──────────────────────────────────────────────

class TestVjudgeRejection:
    def test_vjudge_raw_handle_raises(self):
        ctx = _make_ctx()
        with pytest.raises(cf_common.HandleIsVjudgeError):
            run(cf_common.resolve_handles(ctx, CONVERTER, ('vjudge1',)))

    def test_vjudge_via_dash_c_raises(self):
        ctx = _make_ctx()
        with pytest.raises(cf_common.HandleIsVjudgeError):
            run(cf_common.resolve_handles(ctx, CONVERTER, ('-cvjudge1',)))


# ── Tests: end-to-end stalk scenarios ─────────────────────────────────────

class TestStalkScenarios:
    def test_no_args_default_uses_author_id(self):
        """Simulates ;stalk with no args — code passes '!' + str(ctx.author.id)."""
        ctx = _make_ctx(author=ALICE)
        handles = ('!' + str(ctx.author.id),)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, handles))
        assert result == ['alice_cf']

    def test_no_args_not_confused_by_same_display_name(self):
        """Even if EVE has display_name='alice', self-lookup by ID is unambiguous."""
        ctx = _make_ctx(author=ALICE)
        handles = ('!' + str(ctx.author.id),)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, handles))
        assert result == ['alice_cf']

    def test_mention_in_stalk(self):
        """Simulates ;stalk @bob."""
        ctx = _make_ctx(author=ALICE)
        handles = (f'<@{BOB.id}>',)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, handles))
        assert result == ['bob_cf']

    def test_plain_username_in_stalk(self):
        """Simulates ;stalk bob."""
        ctx = _make_ctx(author=ALICE)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('bob',)))
        assert result == ['bob_cf']

    def test_cf_handle_fallback_in_stalk(self):
        """Simulates ;stalk tourist (no Discord member named tourist)."""
        ctx = _make_ctx(author=ALICE)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('tourist',)))
        assert result == ['tourist']

    def test_force_cf_handle_in_stalk(self):
        """Simulates ;stalk -cbob (force CF handle 'bob', skip Discord lookup)."""
        ctx = _make_ctx(author=ALICE)
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('-cbob',)))
        assert result == ['bob']


# ── Tests: casing (complaint #281) ────────────────────────────────────────

class TestHandleCasing:
    """``;versus tfg Tfg tFg`` is one person, and CF spells them one way."""

    def test_spellings_of_one_handle_collapse(self, patch_user_db):
        ctx = _make_ctx()
        handles = ('tfg', 'Tfg', 'tFg', 'tfG', 'TFG')
        result = run(cf_common.resolve_handles(ctx, CONVERTER, handles))
        assert len(result) == 1
        assert result[0].lower() == 'tfg'

    def test_cached_casing_wins(self, monkeypatch):
        db = FakeUserDb({}, cached=['TFG'])
        monkeypatch.setattr(cf_common, 'user_db', db)
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('tfg', 'Tfg')))
        assert result == ['TFG']

    def test_collapsing_below_mincnt_is_an_error(self, patch_user_db):
        ctx = _make_ctx()
        with pytest.raises(cf_common.HandleCountOutOfBoundsError):
            run(cf_common.resolve_handles(ctx, CONVERTER, ('tfg', 'TFG'),
                                          mincnt=2))

    def test_distinct_handles_are_untouched(self, patch_user_db):
        ctx = _make_ctx()
        result = run(cf_common.resolve_handles(ctx, CONVERTER, ('-ctourist', '-cPetr')))
        assert sorted(result) == ['Petr', 'tourist']

