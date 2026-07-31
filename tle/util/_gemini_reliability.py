"""Pure Gemini error classifiers and request-stat accounting helpers."""


def is_invalid_key_error(status, payload):
    """Return whether a 4xx says the credential itself is unusable."""
    if status not in (400, 401, 403):
        return False
    error = (payload or {}).get('error') or {}
    blob = f"{error.get('status', '')} {error.get('message', '')}".upper()
    return ('API_KEY' in blob or 'API KEY' in blob
            or 'PERMISSION_DENIED' in blob or 'UNAUTHENTICATED' in blob
            or 'CONSUMER_SUSPENDED' in blob)


def is_tool_unsupported_error(status, payload):
    """Return whether a 400 says the selected model rejects a tool."""
    if status != 400:
        return False
    error = (payload or {}).get('error') or {}
    blob = f"{error.get('status', '')} {error.get('message', '')}".lower()
    return 'tool' in blob or 'url_context' in blob or 'google_search' in blob


def is_model_unavailable_error(status, payload):
    """Return whether changing credentials cannot make this model work."""
    if status == 404:
        return True
    if status not in (400, 403):
        return False
    error = (payload or {}).get('error') or {}
    blob = f"{error.get('status', '')} {error.get('message', '')}".lower()
    blob = blob.replace('_', ' ')
    unavailable = ('not found', 'not available', 'unsupported',
                   'not supported', 'does not support', 'no access',
                   'permission denied', 'access denied', 'not authorized')
    return 'model' in blob and any(marker in blob for marker in unavailable)


def model_ladder(models):
    if isinstance(models, str):
        models = [models]
    return list(dict.fromkeys(model for model in models or [] if model))


def record_stats(stats, attempts, payload=None):
    if stats is None:
        return
    stats['attempts'] = stats.get('attempts', 0) + attempts
    usage = (payload or {}).get('usageMetadata') or {}
    fields = {
        'input_tokens': 'promptTokenCount',
        'output_tokens': 'candidatesTokenCount',
        'total_tokens': 'totalTokenCount',
    }
    for target, source in fields.items():
        value = usage.get(source)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            stats[target] = stats.get(target, 0) + value
