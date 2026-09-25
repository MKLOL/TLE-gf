"""Exercise the real HTTP and Discord libraries outside conftest's stubs."""
import subprocess
import sys
from pathlib import Path

import pytest


def test_real_complaint_http_and_command_integration():
    result = subprocess.run(
        [sys.executable, '-m', 'tests.complaint_http_integration'],
        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True,
        timeout=90)
    if result.returncode == 77:
        pytest.skip('Real integration requires aiohttp and discord.py: ' + result.stdout)
    assert result.returncode == 0, result.stdout + result.stderr
