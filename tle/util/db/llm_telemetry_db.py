"""Provider-specific, prompt-free LLM request telemetry."""
import time


class LlmTelemetryDbMixin:
    """Store operational/cost metadata without message or credential data."""

    def _create_llm_telemetry_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS llm_request_usage (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                requested_at      REAL NOT NULL,
                day               TEXT NOT NULL,
                guild_id          TEXT NOT NULL,
                user_id           TEXT NOT NULL,
                provider          TEXT NOT NULL,
                model             TEXT,
                outcome           TEXT NOT NULL,
                router_attempts    INTEGER NOT NULL DEFAULT 0,
                answer_attempts    INTEGER NOT NULL DEFAULT 0,
                input_tokens       INTEGER NOT NULL DEFAULT 0,
                output_tokens      INTEGER NOT NULL DEFAULT 0,
                total_tokens       INTEGER NOT NULL DEFAULT 0,
                latency_ms         INTEGER NOT NULL DEFAULT 0,
                cost_microusd      INTEGER NOT NULL DEFAULT 0,
                context_mode       TEXT,
                context_messages   INTEGER NOT NULL DEFAULT 0
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS llm_request_usage_provider_day
            ON llm_request_usage (provider, day)
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS llm_request_usage_guild_day
            ON llm_request_usage (guild_id, day)
        ''')

    def llm_record_request(self, guild_id, user_id, provider, day, outcome,
                           *, model=None, router_attempts=0,
                           answer_attempts=0, input_tokens=0, output_tokens=0,
                           total_tokens=0, latency_ms=0, cost_microusd=0,
                           context_mode=None, context_messages=0, now=None):
        """Record one invocation; never accepts prompts, answers, or keys."""
        provider = _provider(provider)
        values = (
            time.time() if now is None else float(now), str(day),
            str(guild_id), str(user_id), provider,
            str(model) if model else None, _bounded_text(outcome, 40),
            _nonnegative(router_attempts), _nonnegative(answer_attempts),
            _nonnegative(input_tokens), _nonnegative(output_tokens),
            _nonnegative(total_tokens), _nonnegative(latency_ms),
            _nonnegative(cost_microusd),
            _bounded_text(context_mode, 40) if context_mode else None,
            _nonnegative(context_messages),
        )
        with self.conn:
            cur = self.conn.execute('''
                INSERT INTO llm_request_usage (
                    requested_at, day, guild_id, user_id, provider, model,
                    outcome, router_attempts, answer_attempts, input_tokens,
                    output_tokens, total_tokens, latency_ms, cost_microusd,
                    context_mode, context_messages
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', values)
        return cur.lastrowid

    def llm_provider_summary(self, provider, day, guild_id=None):
        """Aggregate a provider's daily health/cost counters."""
        where = 'provider = ? AND day = ?'
        params = [_provider(provider), str(day)]
        if guild_id is not None:
            where += ' AND guild_id = ?'
            params.append(str(guild_id))
        return self.conn.execute(f'''
            SELECT COUNT(*) AS calls,
                   SUM(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END)
                       AS successes,
                   COALESCE(SUM(router_attempts), 0) AS router_attempts,
                   COALESCE(SUM(answer_attempts), 0) AS answer_attempts,
                   COALESCE(SUM(input_tokens), 0) AS input_tokens,
                   COALESCE(SUM(output_tokens), 0) AS output_tokens,
                   COALESCE(SUM(total_tokens), 0) AS total_tokens,
                   COALESCE(SUM(cost_microusd), 0) AS cost_microusd,
                   COALESCE(AVG(latency_ms), 0) AS average_latency_ms
            FROM llm_request_usage WHERE {where}
        ''', params).fetchone()

    def llm_provider_top_users(self, provider, day, guild_id=None, limit=5):
        where = 'provider = ? AND day = ?'
        params = [_provider(provider), str(day)]
        if guild_id is not None:
            where += ' AND guild_id = ?'
            params.append(str(guild_id))
        params.append(max(1, int(limit)))
        return self.conn.execute(f'''
            SELECT user_id, COUNT(*) AS calls,
                   COALESCE(SUM(cost_microusd), 0) AS cost_microusd
            FROM llm_request_usage WHERE {where}
            GROUP BY user_id ORDER BY calls DESC, user_id LIMIT ?
        ''', params).fetchall()

    def llm_purge_request_usage(self, before_timestamp):
        with self.conn:
            cur = self.conn.execute(
                'DELETE FROM llm_request_usage WHERE requested_at < ?',
                (float(before_timestamp),))
        return cur.rowcount


def _provider(value):
    normalized = str(value or '').strip().lower()
    if normalized not in ('gemini', 'xai'):
        raise ValueError(f'Unsupported LLM telemetry provider: {normalized}')
    return normalized


def _nonnegative(value):
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _bounded_text(value, limit):
    text = ' '.join(str(value or 'unknown').split())
    return text[:limit]
