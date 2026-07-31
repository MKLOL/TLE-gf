import os
import stat
import subprocess


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _write_executable(path, content):
    path.write_text(content)
    path.chmod(0o755)


def test_build_tools_do_not_receive_file_or_inherited_secrets(tmp_path):
    tools = tmp_path / 'tools'
    tools.mkdir()
    events = tmp_path / 'events'
    bot_python = tmp_path / 'bot-python'
    env_file = tmp_path / 'environment'
    env_file.write_text(
        'export BOT_TOKEN="file-token"\n'
        'export XAI_API_KEY="file-xai-secret"\n'
        'export GEMINI_API_KEYS="file-gemini-secret"\n'
        'export VENV_DIR=""\n')
    env_file.chmod(0o600)

    _write_executable(tools / 'git', '''#!/bin/bash
printf 'git:%s:%s\n' "${XAI_API_KEY-unset}" "${BOT_TOKEN-unset}" >> "$TLE_TEST_EVENTS"
''')
    _write_executable(tools / 'poetry', '''#!/bin/bash
printf 'poetry:%s:%s:%s\n' "$*" "${XAI_API_KEY-unset}" "${BOT_TOKEN-unset}" >> "$TLE_TEST_EVENTS"
if [[ "$*" == "env info --executable" ]]; then
    printf '%s\n' "$TLE_TEST_BOT_PYTHON"
fi
''')
    _write_executable(bot_python, '''#!/bin/bash
printf 'bot:%s:%s:%s\n' "${XAI_API_KEY-unset}" "${GEMINI_API_KEYS-unset}" "${BOT_TOKEN-unset}" >> "$TLE_TEST_EVENTS"
kill -TERM "$PPID"
''')

    environment = os.environ.copy()
    environment.update({
        'PATH': f'{tools}:{environment.get("PATH", "")}',
        'TLE_ENV_FILE': str(env_file),
        'TLE_TEST_EVENTS': str(events),
        'TLE_TEST_BOT_PYTHON': str(bot_python),
        'XAI_API_KEY': 'inherited-xai-secret',
        'GEMINI_API_KEYS': 'inherited-gemini-secret',
        'BOT_TOKEN': 'inherited-token',
        'VENV_DIR': '',
    })

    subprocess.run(
        ['bash', os.path.join(_ROOT, 'run.sh')], cwd=_ROOT,
        env=environment, capture_output=True, text=True, timeout=5,
        check=False)

    lines = events.read_text().splitlines()
    tool_lines = [line for line in lines
                  if line.startswith(('git:', 'poetry:'))]
    assert tool_lines
    assert all('xai-secret' not in line and 'token' not in line
               for line in tool_lines)
    assert ('bot:file-xai-secret:file-gemini-secret:file-token' in lines)


def test_launcher_rejects_readable_environment_file(tmp_path):
    env_file = tmp_path / 'environment'
    env_file.write_text('export BOT_TOKEN="secret"\n')
    env_file.chmod(0o644)
    environment = os.environ.copy()
    environment['TLE_ENV_FILE'] = str(env_file)

    result = subprocess.run(
        ['bash', os.path.join(_ROOT, 'run.sh')], cwd=_ROOT,
        env=environment, capture_output=True, text=True, timeout=5,
        check=False)

    assert result.returncode == 1
    assert 'chmod 600' in result.stderr
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o644
