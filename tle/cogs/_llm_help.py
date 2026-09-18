"""Compact, curated help metadata for the LLM command group."""


GROUP_HELP = """**AI — Gemini & Grok**
Ask: `;ai [+gemini|+grok] [+direct|+context] [messages=N] [model] <question>`
Shortcuts: `@gemini <question>` · `@grok <question>`
Explore: `models` · `privacy`
Server controls (mod): `cooldown` · `ratelimit` · `enable` · `disable` · `ban`
Provider controls (owner): `keys` · `grokkeys` · `keystatus` · `grokstatus`
Details: `;help ai <command>` · Quote a reserved-word prompt: `;ai "disable means what?"`"""


COMMAND_HELP = {
    'models': (None, 'List Gemini models, reasoning tiers, and providers.'),
    'keys': ('<key1> [key2 ...]', 'Add Gemini keys. Bot owner only.'),
    'keylist': (None, 'List stored Gemini key fingerprints. Bot owner only.'),
    'keyforget': ('<key_id>', 'Forget one Gemini key. Bot owner only.'),
    'keystatus': (None, 'Show Gemini health and usage. Bot owner only.'),
    'grokkeys': ('<key1> [key2 ...]', 'Add xAI keys. Bot owner only.'),
    'grokkeylist': (None, 'List stored xAI key fingerprints. Bot owner only.'),
    'grokkeyforget': ('<key_id>', 'Forget one xAI key. Bot owner only.'),
    'grokstatus': (None, 'Show Grok health and spend. Bot owner only.'),
    'healthreset': ('<gemini|grok> [key_id] [model]',
                    'Reset reversible provider health. Bot owner only.'),
    'grokreset': (None, 'Reset today\'s Grok guard reservations. Mod only.'),
    'privacy': ('[auto|explicit|off|inherit] [guild|channel]',
                'Show or set how chat context may be forwarded.'),
    'ban': ('<@user|user_id>', 'Ban a user from AI requests here. Mod only.'),
    'unban': ('<@user|user_id>',
              'Remove a user\'s AI request ban here. Mod only.'),
    'banlist': (None, 'List AI request bans for this server. Mod only.'),
    'disable': ('[here] [+threads]',
                'Disable AI server-wide or in a local scope. Mod only.'),
    'enable': ('[here] [+threads]',
               'Enable AI server-wide or in a local scope. Mod only.'),
    'cooldown': ('[seconds] [+threads|+global]',
                 'Show or set shared prompt cooldowns. Use 0 to remove.'),
    'ratelimit': ('[requests] [window] | off | default',
                  'Show or set this server\'s regular-user Grok allowance.'),
}


def command_help(command):
    """Render one concise help card for an LLM subcommand."""
    name = getattr(command, 'name', 'command')
    usage, description = COMMAND_HELP.get(name, (
        getattr(command, 'usage', None),
        getattr(command, 'help', None) or getattr(command, 'brief', None)
        or 'No additional help is available.'))
    signature = f';ai {name}' + (f' {usage}' if usage else '')
    return f'**`{signature}`**\n{description}'


def apply_metadata(group):
    """Attach the same precise metadata to discord.py command objects."""
    commands = getattr(group, 'all_commands', {})
    for name, (usage, description) in COMMAND_HELP.items():
        command = commands.get(name)
        if command is None:
            continue
        command.usage = usage
        command.help = description
