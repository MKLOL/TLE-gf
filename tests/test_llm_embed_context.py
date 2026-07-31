"""Tests for embed-aware context and the context-preferring router.

Covers the behaviour added in MKLOL/TLE-gf#11: the bot reading the rendered
text of embeds (its own answers are embeds, so a reply to one used to quote an
empty string), keeping the direct reply target even when it is the bot, and
the prompt shapes that follow from routing to context by default.
"""
from datetime import datetime, timedelta, timezone

from tle.cogs import _llm_context as llm_context
from tle.cogs import _llm_history as llm_history
from tle.cogs import _llm_pipeline as llm_pipeline

_BASE = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


class FakeEmbed:
    """The attributes ``_embed_text`` reads off a discord.Embed."""

    def __init__(self, title=None, description=None, fields=(),
                 author=None, footer=None, url=None):
        self.title = title
        self.description = description
        self.url = url
        self.fields = [type('F', (), {'name': n, 'value': v})()
                       for n, v in fields]
        self.author = type('A', (), {'name': author})() if author else None
        self.footer = type('Ft', (), {'text': footer})() if footer else None


class EmbedMessage:
    def __init__(self, author='rin', content='', embeds=(), offset=0,
                 attachments=(), is_bot=False, author_id=1):
        self.content = content
        self.embeds = list(embeds)
        self.attachments = list(attachments)
        self.created_at = _BASE + timedelta(seconds=offset)
        self.author = type('A', (), {'display_name': author, 'bot': is_bot,
                                     'id': author_id})()


class TestMessageText:
    def test_plain_content_is_returned_as_is(self):
        assert llm_history.message_text(
            EmbedMessage(content='use a BIT')) == 'use a BIT'

    def test_an_embed_description_is_visible(self):
        message = EmbedMessage(
            embeds=[FakeEmbed(description='a segment tree is...')])
        assert 'a segment tree is...' in llm_history.message_text(message)

    def test_embed_title_author_fields_and_footer_all_appear(self):
        message = EmbedMessage(embeds=[FakeEmbed(
            title='Answer', description='body', author='Rin',
            fields=[('Model', 'gemini-3.5-flash-lite')], footer='asked by nife')])
        text = llm_history.message_text(message)
        for expected in ('Answer', 'body', 'Rin', 'gemini-3.5-flash-lite',
                         'asked by nife'):
            assert expected in text

    def test_a_bare_embed_url_is_kept_when_nothing_else_is(self):
        message = EmbedMessage(embeds=[FakeEmbed(url='https://example.com')])
        assert 'https://example.com' in llm_history.message_text(message)

    def test_attachments_are_named(self):
        message = EmbedMessage(
            attachments=[type('At', (), {'filename': 'wa.png'})()])
        assert '[attached: wa.png]' in llm_history.message_text(message)

    def test_an_unnamed_attachment_still_registers(self):
        message = EmbedMessage(attachments=[type('At', (), {})()])
        assert '[attached: file]' in llm_history.message_text(message)

    def test_a_message_with_nothing_in_it_is_empty(self):
        assert llm_history.message_text(EmbedMessage()) == ''

    def test_none_is_tolerated(self):
        # build_prompt calls this on a possibly-deleted reference.
        assert llm_history.message_text(None) == ''


class TestEmbedOnlyMessagesAreUsable:
    def test_an_embed_only_message_counts_as_content(self):
        message = EmbedMessage(embeds=[FakeEmbed(description='hi')])
        assert llm_history._is_usable(message) is True

    def test_the_bot_is_still_skipped_by_default(self):
        message = EmbedMessage(embeds=[FakeEmbed(description='hi')],
                               is_bot=True, author_id=99)
        assert llm_history._is_usable(message, bot_user_id=99) is False

    def test_the_focused_reply_target_survives_being_the_bot(self):
        # The whole point of #11: replying to the bot's own answer embed.
        message = EmbedMessage(embeds=[FakeEmbed(description='hi')],
                               is_bot=True, author_id=99)
        assert llm_history._is_usable(message, bot_user_id=99,
                                      include_bot=True) is True


class TestTranscriptIncludesEmbeds:
    def test_the_bots_answer_reaches_the_transcript(self):
        window = [EmbedMessage(author='nife', content='what is a BIT?'),
                  EmbedMessage(author='Rin', offset=1, is_bot=True,
                               embeds=[FakeEmbed(description='a Fenwick tree')])]
        transcript = llm_history.format_transcript(window)
        assert 'what is a BIT?' in transcript
        assert 'a Fenwick tree' in transcript

    def test_the_focus_marker_names_the_replied_to_message(self):
        target = EmbedMessage(author='Rin', content='a Fenwick tree')
        transcript = llm_history.format_transcript([target], focus=target)
        assert 'being replied to' in transcript


class TestContextPromptShape:
    def test_a_plain_context_question_claims_no_replied_to_message(self):
        # Regression: this sentence used to be unconditional, so every
        # non-reply context question pointed the model at a marker that the
        # transcript does not contain.
        prompt = llm_pipeline.build_prompt(
            'who is right?', None,
            [EmbedMessage(author='nife', content='use a BIT')],
            mode=llm_context.MODE_CONTEXT)
        assert 'replied-to message' not in prompt
        assert 'The user asks: who is right?' in prompt

    def test_a_reply_with_a_window_does_name_the_quoted_message(self):
        referenced = EmbedMessage(author='Rin',
                                  embeds=[FakeEmbed(description='use a BIT')])
        prompt = llm_pipeline.build_prompt(
            'why?', referenced, [referenced],
            mode=llm_context.MODE_REPLY_CHAIN)
        assert 'replied-to message' in prompt
        assert 'use a BIT' in prompt

    def test_replying_to_an_embed_quotes_its_rendered_text(self):
        # Without message_text this quoted '(empty message)': the answer lives
        # in the embed, and message.content is blank.
        referenced = EmbedMessage(author='Rin',
                                  embeds=[FakeEmbed(description='a Fenwick tree')])
        prompt = llm_pipeline.build_prompt('why?', referenced, [])
        assert 'a Fenwick tree' in prompt
        assert '(empty message)' not in prompt


class TestMissingContextIsAdmitted:
    def test_a_direct_question_is_still_bare(self):
        assert llm_pipeline.build_prompt('what is a BIT?', None, [],
                                         mode=llm_context.MODE_DIRECT) == \
            'what is a BIT?'

    def test_context_wanted_but_unavailable_says_so(self):
        # Reachable whenever the router asks for context and the gather comes
        # back empty — a quiet channel, or a lost Read Message History.
        prompt = llm_pipeline.build_prompt('what is a BIT?', None, [],
                                           mode=llm_context.MODE_CONTEXT)
        assert 'No transcript' in prompt
        assert 'what is a BIT?' in prompt

    def test_the_question_survives_the_wrapper(self):
        prompt = llm_context.build_question_prompt('  hi?  ',
                                                   context_requested=True)
        assert prompt.endswith('hi?')
