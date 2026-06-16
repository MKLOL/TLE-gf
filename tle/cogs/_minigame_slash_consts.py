"""Slash-command choice constants for the minigames cog."""

from discord import app_commands

_TIMEFRAME_CHOICES = [
    app_commands.Choice(name='This week', value='week'),
    app_commands.Choice(name='This month', value='month'),
    app_commands.Choice(name='This year', value='year'),
]

_MODE_CHOICES = [
    app_commands.Choice(name='Raw (time only)', value='raw'),
    app_commands.Choice(name='All puzzles', value='all'),
]
