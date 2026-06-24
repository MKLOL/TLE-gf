"""Tests for unknown-subcommand detection in the ;bet group."""


class TestUnknownSubcommandToken:
    """Recovers the attempted subcommand from the raw message, since discord.py
    wipes ctx.subcommand_passed before the group callback runs."""

    def _ctx(self, content, *, prefix=';', invoked='bet',
             known=('home', 'draw', 'away', '1', 'x', '2', 'mybet')):
        class _Cmd:
            all_commands = {name: 1 for name in known}

        class _Ctx:
            pass
        ctx = _Ctx()
        ctx.prefix = prefix
        ctx.invoked_with = invoked
        ctx.command = _Cmd()

        class _Msg:
            pass
        ctx.message = _Msg()
        ctx.message.content = content
        return ctx

    def test_unknown_token_returned(self):
        from tle.cogs._betting_helpers import unknown_subcommand_token
        assert unknown_subcommand_token(self._ctx(';bet junk')) == 'junk'
        assert unknown_subcommand_token(self._ctx(';bet all34')) == 'all34'
        assert unknown_subcommand_token(self._ctx(';bet all 1')) == 'all'

    def test_bare_invocation_is_none(self):
        from tle.cogs._betting_helpers import unknown_subcommand_token
        assert unknown_subcommand_token(self._ctx(';bet')) is None
        assert unknown_subcommand_token(self._ctx(';bet   ')) is None

    def test_valid_subcommand_and_alias_are_none(self):
        from tle.cogs._betting_helpers import unknown_subcommand_token
        assert unknown_subcommand_token(self._ctx(';bet home 100')) is None
        assert unknown_subcommand_token(self._ctx(';bet 1 50')) is None
        assert unknown_subcommand_token(self._ctx(';bet MyBet')) is None  # case-insensitive

    def test_mention_prefix_and_alias_group_name(self):
        from tle.cogs._betting_helpers import unknown_subcommand_token
        # invoked via the `prediction` alias of the group
        assert unknown_subcommand_token(
            self._ctx(';prediction xyz', invoked='prediction')) == 'xyz'
        # mention prefix instead of ';'
        assert unknown_subcommand_token(
            self._ctx('<@!999> bet junk', prefix='<@!999> ')) == 'junk'
