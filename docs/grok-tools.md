# Grok message lookup

Enable with `;meta config enable grok_tools`; disable with `;meta config disable grok_tools`.
It is off by default and uses existing xAI credentials. No migration or new dependency.

`@grok What is <@USER_ID>'s favorite food?` can search, read the conversation, then
answer in Grok's existing voice. Links are optional. Mention users or supply IDs;
v1 does not resolve names. Both `@grok` and `;ai +grok` work. Images use the existing pipeline.

The only tools are `search_messages(query?, author?, after?, before?)` and
`read_message(message_ref)` (an ID). They read only the invoking channel/thread.
Search returns up to five matches with one neighbor each side; read expands with
two each side. Records have authors, dates, text, links, and match/truncation flags.
Search defaults to seven days; explicit ISO dates can reach older history. Dates
default to UTC. Errors and partial results are not evidence that nothing exists.

Limits per request: five model turns, four serial lookups, six provider attempts,
2,500 output tokens including reasoning, 60,000 cumulative input bytes, and 6,000
retrieved characters. Each lookup has eight seconds; the existing overall deadline
and credit guards apply. Unknown provider usage is conservatively accounted for.

Existing privacy, bans, disables, cooldowns, and `messages=N` controls apply.
`+direct` disables lookup. Both user and bot need history access, including private
thread membership. Discord's Message Content intent must be enabled for search.
Only explicitly replied-to text remains available when history forwarding is off.
Retrieved secrets are redacted; message text is evidence, never tool instructions.

Run `python3 -m pytest tests/ -q`. Discord and xAI are mocked; live model search
quality and API compatibility require a later smoke test with credentials.
