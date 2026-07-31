import asyncio
import importlib.util
import logging
import os
import sys
import types

import pytest


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_discord_common(monkeypatch):
    commands = sys.modules['discord.ext.commands']
    db_module = sys.modules['tle.util.db']
    cf_module = sys.modules['tle.util.codeforces_api']
    defaults = {
        'DefaultHelpCommand': type('DefaultHelpCommand', (), {}),
        'NoPrivateMessage': type('NoPrivateMessage', (Exception,), {}),
        'DisabledCommand': type('DisabledCommand', (Exception,), {}),
        'UserInputError': type('UserInputError', (Exception,), {}),
    }
    for name, value in defaults.items():
        monkeypatch.setattr(commands, name, value, raising=False)
    monkeypatch.setattr(
        db_module, 'DatabaseDisabledError',
        type('DatabaseDisabledError', (Exception,), {}), raising=False)
    monkeypatch.setattr(
        cf_module, 'CodeforcesApiError',
        type('CodeforcesApiError', (Exception,), {}), raising=False)

    name = 'test_actual_discord_common'
    path = os.path.join(_ROOT, 'tle', 'util', 'discord_common.py')
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def _load_logging_cog(monkeypatch, discord_common):
    util = sys.modules['tle.util']
    monkeypatch.setattr(util, 'discord_common', discord_common, raising=False)
    name = 'test_actual_logging_cog'
    path = os.path.join(_ROOT, 'tle', 'cogs', 'logging.py')
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('subcommand', (
    'keys', 'grokkeys', 'xkeys', 'xaikeys',
))
@pytest.mark.parametrize('root', ('llm', 'ai'))
def test_key_command_arguments_are_always_redacted(
        monkeypatch, subcommand, root):
    common = _load_discord_common(monkeypatch)
    raw = f';{root} {subcommand} totally-unfamiliar-secret-format\nsecond-secret'

    safe = common.redact_credentials(raw)

    assert safe == f';{root} {subcommand} [credentials redacted]'
    assert 'unfamiliar' not in safe
    assert 'second-secret' not in safe


def test_provider_keys_are_redacted_without_censoring_ordinary_text(monkeypatch):
    common = _load_discord_common(monkeypatch)
    xai_key = 'xai-' + 'A1_b' * 8
    gemini_key = 'AIza' + 'Z9-x' * 8
    text = f'failed with `{xai_key}` and query_key={gemini_key}.'

    safe = common.redact_credentials(text)

    assert xai_key not in safe
    assert gemini_key not in safe
    assert safe.count('[credentials redacted]') == 2
    ordinary = 'The xai-api supports an AIza prefix; xai-short is not a key.'
    assert common.redact_credentials(ordinary) == ordinary


@pytest.mark.parametrize('root', ('llm', 'ai'))
def test_mention_prefix_key_command_redacts_the_entire_tail(monkeypatch, root):
    common = _load_discord_common(monkeypatch)
    raw = f'<@!123456789> {root} grokkeys unfamiliar-format\nsecond-line'
    safe = common.redact_credentials(raw)
    assert safe.endswith('[credentials redacted]')
    assert 'unfamiliar-format' not in safe
    assert 'second-line' not in safe


def test_redacting_formatter_covers_message_and_exception(monkeypatch):
    common = _load_discord_common(monkeypatch)
    key = 'xai-' + 'sensitive' * 4
    try:
        raise RuntimeError(f'provider echoed {key}')
    except RuntimeError as error:
        record = logging.LogRecord(
            'provider', logging.ERROR, __file__, 1,
            'request failed for %s', (key,),
            (type(error), error, error.__traceback__))
    rendered = common.RedactingFormatter('%(message)s').format(record)
    assert key not in rendered
    assert rendered.count('[credentials redacted]') >= 2


def test_command_error_log_record_never_contains_raw_command(monkeypatch):
    common = _load_discord_common(monkeypatch)
    key = 'xai-' + 'sensitive' * 4
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = Capture()
    common.logger.addHandler(handler)
    common.logger.setLevel(logging.ERROR)
    ctx = types.SimpleNamespace(
        command='llm grokkeys',
        message=types.SimpleNamespace(
            content=f';llm grokkeys {key}', jump_url='https://discord.test/1'),
    )
    try:
        asyncio.run(common.bot_error_handler(ctx, RuntimeError('storage failed')))
    finally:
        common.logger.removeHandler(handler)

    assert len(records) == 1
    assert records[0].message_content == (
        ';llm grokkeys [credentials redacted]')
    assert key not in records[0].message_content


def test_discord_logging_redacts_command_ui_and_trace_text(monkeypatch):
    common = _load_discord_common(monkeypatch)
    logging_cog = _load_logging_cog(monkeypatch, common)
    key = 'AIza' + 'SensitiveKey' * 3
    record = logging.LogRecord(
        'provider', logging.ERROR, __file__, 1,
        'provider rejected %s', (key,), None,
    )
    record.message_content = f';llm keys {key}'
    record.jump_url = 'https://discord.test/2'

    class OneRecordQueue:
        def __init__(self):
            self.used = False

        async def get(self):
            if self.used:
                raise StopAsyncIteration
            self.used = True
            return record

    class Channel:
        def __init__(self):
            self.sent = []

        async def send(self, value=None, *, embed=None):
            self.sent.append(embed if embed is not None else value)

    channel = Channel()
    bot = types.SimpleNamespace(get_channel=lambda channel_id: channel)
    cog = logging_cog.Logging(bot, 123)
    cog.queue = OneRecordQueue()
    cog.setFormatter(logging.Formatter('%(message)s'))

    with pytest.raises(StopAsyncIteration):
        asyncio.run(cog._log_task())

    rendered = '\n'.join(
        item.description if hasattr(item, 'description') else str(item)
        for item in channel.sent)
    assert key not in rendered
    assert '**Original Command:** ;llm keys [credentials redacted]' in rendered
    assert 'provider rejected [credentials redacted]' in rendered
