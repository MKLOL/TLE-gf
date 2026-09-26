"""Answer "is this commit part of the running code?" for deploy-gated work.

Pushing a fix is not deploying it. Anything that promises a user "this is
fixed" must wait until the checkout the bot is actually running from contains
the commit, which is the same question ``;meta git`` answers from ``git`` in
the process's working directory.
"""
import os
import re
import subprocess

_COMMIT_URL_SHA = re.compile(r'/commit/([0-9a-fA-F]{7,40})(?:[/?#]|$)')
_GIT_TIMEOUT = 10


def commit_sha(commit_url):
    """Extract the SHA from a GitHub commit URL, or None."""
    match = _COMMIT_URL_SHA.search(commit_url or '')
    return match.group(1).lower() if match else None


def _minimal_env():
    # Same minimal environment ;meta git uses, so both agree on which
    # repository "the running code" means.
    env = {}
    for key in ('SYSTEMROOT', 'PATH'):
        value = os.environ.get(key)
        if value is not None:
            env[key] = value
    env['LANGUAGE'] = 'C'
    env['LANG'] = 'C'
    env['LC_ALL'] = 'C'
    return env


def current_head(cwd=None):
    """The full SHA of HEAD in the bot's checkout, or None if unknowable.

    Captured once at process start it names the code actually running: a
    later ``git pull`` moves HEAD but not the loaded modules, so deploy
    checks compare against this, never against live HEAD.
    """
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'], cwd=cwd, env=_minimal_env(),
            capture_output=True, text=True, timeout=_GIT_TIMEOUT)
    except (OSError, subprocess.TimeoutExpired):
        return None
    sha = result.stdout.strip().lower()
    if result.returncode != 0 or not re.fullmatch(r'[0-9a-f]{40}', sha):
        return None
    return sha


def commit_is_deployed(sha, head, cwd=None):
    """True when ``sha`` is an ancestor of ``head`` in the bot's checkout.

    Anything that is not a clean "yes" — unknown SHA, no git, no repository,
    a timeout — is "no": the caller is deciding whether to tell someone their
    problem is fixed, and guessing wrong in that direction is the bad one.
    """
    if not sha or not head:
        return False
    try:
        result = subprocess.run(
            ['git', 'merge-base', '--is-ancestor', sha, head],
            cwd=cwd, env=_minimal_env(), stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, timeout=_GIT_TIMEOUT)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0
