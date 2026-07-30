import logging

from discord.ext import commands

from tle.cogs._gemini_context import (
    ANSWER_SYSTEM_PROMPT,
    CLASSIFIER_GENERATION_CONFIG,
    CLASSIFIER_SYSTEM_PROMPT,
    DIRECT,
    REQUIRES_CONTEXT,
    GeminiClassificationError,
    answer_prompt,
    classifier_prompt,
    collect_recent_messages,
    collect_reply_context,
    parse_classification,
    repair_classifier_prompt,
)
from tle.util import discord_common
from tle.util.gemini_api import (
    GeminiClient,
    GeminiError,
    GeminiNoQuotaError,
    GeminiResult,
    parse_command_request,
)
from tle.util.gemini_keys import GeminiKeyPool


logger = logging.getLogger(__name__)

_MESSAGE_LIMIT = 2000


class GeminiCogError(commands.CommandError):
    pass


class Gemini(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        try:
            key_pool = GeminiKeyPool.from_environment()
        except ValueError as exc:
            self.client = None
            self.configuration_error = str(exc)
        else:
            self.client = GeminiClient(key_pool)
            self.configuration_error = None

    async def cog_unload(self):
        if self.client is not None:
            await self.client.close()

    @commands.command(
        name='ai',
        brief='Ask Gemini a question',
        usage='[model[-reasoning]] <query>',
    )
    async def gemini(self, ctx, *, request: str):
        """Send a query to Gemini.

        Without a model, Gemini 3.1 Flash Lite is tried first and Gemini 3.5
        Flash Lite is the fallback. Add a reasoning tier to a model with a
        final hyphen, for example `3.1-flash-lite-low`. Recent channel context
        or context centered on a replied-to message is included automatically
        when the request depends on it.

        Usage:
          ;ai <query>
          ;ai 3.1-flash-lite-low <query>
          ;ai gemini-2.5-flash-off <query>

        Models and reasoning tiers:
          2.5-flash, 2.5-flash-lite: off, low, medium, high
          3-flash, 3.1-flash-lite, 3.5-flash,
          3.5-flash-lite, 3.6-flash: minimal, low, medium, high
        """
        if self.client is None:
            raise GeminiCogError(self.configuration_error)

        try:
            model_requests, query = parse_command_request(request)
        except GeminiError as exc:
            raise GeminiCogError(str(exc)) from exc
        logger.info(
            'Gemini command invoked by %s in guild %s; requested models: %s',
            ctx.author.id,
            ctx.guild.id,
            ', '.join(item.model.api_name for item in model_requests),
        )

        async with ctx.typing():
            try:
                result = await self._respond(ctx, query, model_requests)
            except GeminiNoQuotaError:
                logger.info(
                    'No Gemini API quota left for command from %s in guild %s',
                    ctx.author.id,
                    ctx.guild.id,
                )
                await ctx.send('No API Quota left')
                return
            except GeminiError as exc:
                raise GeminiCogError(str(exc)) from exc

        reasoning = result.reasoning or 'default'
        heading = (
            f'**{result.model.display_name}** '
            f'(`{reasoning}` reasoning)\n'
        )
        await _send_response(ctx, _split_message(heading + result.text))

    async def _respond(self, ctx, query, model_requests):
        is_reply = (
            ctx.message.reference is not None
            and ctx.message.reference.message_id is not None
        )
        classification_result = await self.client.generate(
            classifier_prompt(ctx, query, is_reply=is_reply),
            model_requests,
            system_prompt=CLASSIFIER_SYSTEM_PROMPT,
            generation_config=CLASSIFIER_GENERATION_CONFIG,
        )
        try:
            classification = parse_classification(
                classification_result.text,
                is_reply=is_reply,
            )
        except GeminiClassificationError as first_error:
            repair_result = await self.client.generate(
                repair_classifier_prompt(
                    ctx,
                    query,
                    is_reply=is_reply,
                    output=classification_result.text,
                    error=first_error,
                ),
                model_requests,
                system_prompt=CLASSIFIER_SYSTEM_PROMPT,
                generation_config=CLASSIFIER_GENERATION_CONFIG,
            )
            try:
                classification = parse_classification(
                    repair_result.text,
                    is_reply=is_reply,
                )
            except GeminiClassificationError as second_error:
                raise GeminiError(
                    'Gemini returned invalid classifier output twice: '
                    f'{second_error}.'
                ) from second_error
            classification_result = repair_result

        logger.info(
            'Gemini classified command from %s as %s',
            ctx.author.id,
            classification.response_type,
        )
        if classification.response_type == DIRECT:
            return GeminiResult(
                classification.message,
                classification_result.model,
                classification_result.reasoning,
            )

        if classification.response_type == REQUIRES_CONTEXT:
            messages = await collect_recent_messages(ctx)
        else:
            messages = await collect_reply_context(ctx)
        logger.info(
            'Gemini collected %s message(s) for %s',
            len(messages),
            classification.response_type,
        )
        return await self.client.generate(
            answer_prompt(ctx, query, messages),
            model_requests,
            system_prompt=ANSWER_SYSTEM_PROMPT,
        )

    @discord_common.send_error_if(GeminiCogError)
    async def cog_command_error(self, ctx, error):
        pass


def _split_message(text):
    """Split a Gemini response without exceeding Discord's message limit."""
    messages = []
    remaining = text
    while len(remaining) > _MESSAGE_LIMIT:
        split_at = remaining.rfind('\n', 0, _MESSAGE_LIMIT + 1)
        if split_at <= 0:
            split_at = _MESSAGE_LIMIT
        messages.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip('\n')
    if remaining:
        messages.append(remaining)
    return messages


async def _send_response(ctx, messages):
    sent = await ctx.reply(messages[0], mention_author=False)
    for message in messages[1:]:
        sent = await sent.reply(message, mention_author=False)


async def setup(bot):
    await bot.add_cog(Gemini(bot))
