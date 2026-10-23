"""Hard per-invocation limits for the small, text-only Grok tool loop."""
import json
import math

from tle import constants

MAX_TOOLS = 4
MAX_TURNS = MAX_TOOLS + 1
MAX_ATTEMPTS = 6
MAX_OUTPUT_TOKENS = 2500
# UTF-8 request bytes conservatively bound text input, including tool schemas
# and repeated conversation turns. Image requests use the existing path.
MAX_INPUT_BYTES = 60000


class BudgetExhausted(Exception):
    pass


def _count(value):
    if (not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(value) or value < 0):
        return None
    return math.ceil(value)


def reservation_microusd():
    return math.ceil(
        MAX_INPUT_BYTES * constants.XAI_INPUT_USD_PER_MILLION
        + MAX_OUTPUT_TOKENS * constants.XAI_OUTPUT_USD_PER_MILLION)


class RequestBudget:
    def __init__(self, stats):
        self.stats = stats
        self.stats.setdefault('accounted_microusd', 0)
        self.attempts = self.input_bytes = self.output_tokens = 0
        self.pending = None

    @property
    def remaining_output(self):
        return max(0, MAX_OUTPUT_TOKENS - self.output_tokens)

    def claim(self, payload):
        size = len(json.dumps(payload, ensure_ascii=False).encode('utf-8')) + 256
        if (self.attempts >= MAX_ATTEMPTS or self.remaining_output < 128
                or self.input_bytes + size > MAX_INPUT_BYTES):
            raise BudgetExhausted
        cap = min(payload['max_output_tokens'], self.remaining_output)
        payload['max_output_tokens'] = cap
        self.attempts += 1
        self.input_bytes += size
        self.output_tokens += cap
        self.pending = size, cap

    def observe(self, usage_stats):
        """Keep an estimate for every unobserved attempt, even after success."""
        size, cap = self.pending
        self.pending = None
        used = _count(usage_stats.get('output_tokens'))
        if used is not None:
            self.output_tokens += used - cap
        cost = _count(usage_stats.get('cost_microusd'))
        if cost is None:
            input_count = _count(usage_stats.get('input_tokens'))
            cost = math.ceil(
                (size if input_count is None else input_count)
                * constants.XAI_INPUT_USD_PER_MILLION
                + (cap if used is None else used)
                * constants.XAI_OUTPUT_USD_PER_MILLION)
        self.stats['accounted_microusd'] = (
            self.stats.get('accounted_microusd', 0) + cost)
