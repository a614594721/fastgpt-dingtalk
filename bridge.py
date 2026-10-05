"""DingTalk Stream bridge with message reactions and incremental FastGPT replies."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections import OrderedDict
from typing import Any, AsyncIterator

import dingtalk_stream
import httpx
from dingtalk_stream import AckMessage
from markdown_it import MarkdownIt

import dingtalk_fastgpt as upstream


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fastgpt-dingtalk")

DINGTALK_API_BASE_URL = "https://api.dingtalk.com"
REACTION_EMOJI = os.getenv("DINGTALK_REACTION_EMOJI", "🤔Thinking").strip()
DONE_EMOJI = os.getenv("DINGTALK_DONE_EMOJI", "✅Done").strip()
CARD_TEMPLATE_ID = os.getenv("DINGTALK_CARD_TEMPLATE_ID", "").strip()
CARD_ENABLED = os.getenv("DINGTALK_CARD_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
CARD_TEMPLATE_KEY = os.getenv("DINGTALK_CARD_TEMPLATE_KEY", "content").strip() or "content"
CARD_INTERVAL = 0.5
MARKDOWN = MarkdownIt("commonmark").enable("table")


def parse_sse_data(data: str) -> str:
    """Extract answer text from an OpenAI-compatible SSE event, excluding reasoning."""
    if data == "[DONE]":
        return ""
    event = json.loads(data)
    if error := event.get("error"):
        raise RuntimeError(f"FastGPT stream error: {error}")
    content = "".join(
        part.get("delta", {}).get("content", "") or ""
        for part in event.get("choices", [])
        if isinstance(part.get("delta", {}).get("content", ""), str)
    )
    return content


def format_dingtalk_markdown(content: str) -> str:
    """Replace GFM tables with readable field lists before DingTalk renders Markdown."""
    lines = content.splitlines()
    replacements: list[tuple[int, int, list[str]]] = []
    tokens = MARKDOWN.parse(content)
    for index, token in enumerate(tokens):
        if token.type != "table_open" or not token.map:
            continue
        headers: list[str] = []
        rows: list[list[str]] = []
        row: list[str] | None = None
        for child in tokens[index + 1 :]:
            if child.type == "table_close":
                break
            if child.type == "tr_open":
                row = []
            elif child.type == "inline" and row is not None:
                row.append(child.content)
            elif child.type == "tr_close" and row is not None:
                if not headers:
                    headers = row
                else:
                    rows.append(row)
                row = None
        converted = []
        for values in rows:
            converted.extend(
                f"- **{header}：** {value}"
                for header, value in zip(headers, values)
                if header or value
            )
            converted.append("")
        replacements.append((token.map[0], token.map[1], converted))
    for start, end, converted in reversed(replacements):
        lines[start:end] = converted
    return "\n".join(lines)


async def stream_fastgpt(client: httpx.AsyncClient, question: str, chat_id: str) -> AsyncIterator[str]:
    """Yield only user-visible content; fail on HTTP and malformed SSE responses."""
    payload = {
        "appId": upstream.FASTGPT_APP_ID,
        "chatId": chat_id,
        "stream": True,
        "detail": upstream.FASTGPT_DETAIL,
        "messages": [{"role": "user", "content": question}],
    }
    headers = {"Authorization": f"Bearer {upstream.FASTGPT_API_KEY}"}
    timeout = httpx.Timeout(upstream.FASTGPT_TIMEOUT, connect=10)
    async with client.stream(
        "POST",
        f"{upstream.FASTGPT_BASE_URL}/api/v1/chat/completions",
        json=payload,
        headers=headers,
        timeout=timeout,
    ) as response:
        response.raise_for_status()
        if "text/event-stream" not in response.headers.get("content-type", ""):
            raise RuntimeError("FastGPT did not return an SSE stream")
        data_lines: list[str] = []
        async for line in response.aiter_lines():
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif not line and data_lines:
                data = "\n".join(data_lines)
                data_lines.clear()
                if data == "[DONE]":
                    return
                if text := parse_sse_data(data):
                    yield text
        if data_lines:
            if text := parse_sse_data("\n".join(data_lines)):
                yield text


class DingTalkApi:
    """Cache DingTalk credentials and send reactions, cards, and webhook replies."""

    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self._token = ""
        self._expires_at = 0.0
        self._token_lock = asyncio.Lock()

    async def access_token(self) -> str:
        async with self._token_lock:
            if self._token and time.monotonic() < self._expires_at:
                return self._token
            response = await self.client.post(
                f"{DINGTALK_API_BASE_URL}/v1.0/oauth2/accessToken",
                json={"appKey": upstream.DINGTALK_APP_KEY, "appSecret": upstream.DINGTALK_APP_SECRET},
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()
            token = result.get("accessToken")
            if not token:
                raise RuntimeError(f"DingTalk access token missing: {result}")
            self._token = token
            self._expires_at = time.monotonic() + max(60, int(result.get("expireIn") or 7200) - 300)
            return token

    async def request(self, method: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        token = await self.access_token()
        response = await self.client.request(
            method,
            f"{DINGTALK_API_BASE_URL}{path}",
            headers={"x-acs-dingtalk-access-token": token},
            json=payload,
            timeout=15,
        )
        result = response.json() if response.content else {}
        if response.is_error:
            raise RuntimeError(
                f"DingTalk {path} HTTP {response.status_code}: "
                f"code={result.get('code')}, message={result.get('message')}, "
                f"requestid={result.get('requestid')}"
            )
        if result.get("success") is False:
            raise RuntimeError(f"DingTalk {path} failed: {result}")
        return result

    async def emotion(self, message: dingtalk_stream.ChatbotMessage, emoji: str, recall: bool) -> None:
        if not emoji or emoji.lower() == "none" or not message.message_id or not message.conversation_id:
            return
        await self.request(
            "POST",
            "/v1.0/robot/emotion/recall" if recall else "/v1.0/robot/emotion/reply",
            {
                "robotCode": message.robot_code or upstream.DINGTALK_APP_KEY,
                "openMsgId": message.message_id,
                "openConversationId": message.conversation_id,
                "emotionType": 2,
                "emotionName": emoji,
                "textEmotion": {
                    "emotionId": "2659900",
                    "emotionName": emoji,
                    "text": emoji,
                    "backgroundId": "im_bg_1",
                },
            },
        )

    async def reply_text(self, message: dingtalk_stream.ChatbotMessage, text: str) -> None:
        formatted = format_dingtalk_markdown(text)
        title = next((line.strip("# *>")[:60] for line in formatted.splitlines() if line.strip()), "FastGPT 回复")
        payload: dict[str, Any] = {
            "msgtype": "markdown",
            "markdown": {"title": title or "FastGPT 回复", "text": formatted},
        }
        if message.sender_staff_id:
            payload["at"] = {"atUserIds": [message.sender_staff_id]}
        response = await self.client.post(message.session_webhook, json=payload, timeout=15)
        response.raise_for_status()
        result = response.json()
        if result.get("errcode", 0) != 0:
            raise RuntimeError(f"DingTalk markdown reply failed: {result}")

    async def create_card(self, message: dingtalk_stream.ChatbotMessage) -> str:
        out_track_id = f"fastgpt_{uuid.uuid4().hex}"
        is_group = str(message.conversation_type) == "2"
        logger.info(
            "creating AI card, group=%s, conversation_present=%s, sender_present=%s",
            is_group, bool(message.conversation_id), bool(message.sender_staff_id)
        )
        open_space_id = (
            f"dtv1.card//IM_GROUP.{message.conversation_id}"
            if is_group
            else f"dtv1.card//im_robot.{message.sender_staff_id}"
        )
        deliver = (
            {"imGroupOpenDeliverModel": {"robotCode": message.robot_code or upstream.DINGTALK_APP_KEY}}
            if is_group
            else {
                "imRobotOpenDeliverModel": {
                    "spaceType": "IM_ROBOT",
                    "robotCode": message.robot_code or upstream.DINGTALK_APP_KEY,
                }
            }
        )
        payload = {
            "cardTemplateId": CARD_TEMPLATE_ID,
            "outTrackId": out_track_id,
            "cardData": {"cardParamMap": {CARD_TEMPLATE_KEY: "正在处理..."}},
            "callbackType": "STREAM",
            "openSpaceId": open_space_id,
            "userIdType": 1,
            "imGroupOpenSpaceModel": {"supportForward": True},
            "imRobotOpenSpaceModel": {
                "supportForward": True,
                "lastMessageI18n": {"ZH_CN": "正在处理..."}
            },
            **({"userId": message.sender_staff_id} if not is_group else {}),
            **deliver,
        }
        result = await self.request(
            "POST",
            "/v1.0/card/instances/createAndDeliver",
            payload,
        )
        for delivery in result.get("result", {}).get("deliverResults", []):
            if not delivery.get("success"):
                raise RuntimeError(f"DingTalk card delivery failed: {delivery.get('errorMsg')}")
        return result.get("result", {}).get("outTrackId") or result.get("outTrackId") or out_track_id

    async def update_card(self, out_track_id: str, content: str, finalize: bool) -> None:
        await self.request(
            "PUT",
            "/v1.0/card/streaming",
            {
                "outTrackId": out_track_id,
                "key": CARD_TEMPLATE_KEY,
                "content": content,
                "isFull": True,
                "isFinalize": finalize,
                "isError": False,
                "guid": str(uuid.uuid4()),
            },
        )


class ReplyStream:
    """Update a configured AI card or collect one final text reply."""

    def __init__(self, api: DingTalkApi, message: dingtalk_stream.ChatbotMessage, card_id: str = ""):
        self.api = api
        self.message = message
        self.card_id = card_id
        self.answer = ""
        self.last_sent_at = 0.0

    async def add(self, delta: str) -> None:
        self.answer += delta
        now = time.monotonic()
        if self.card_id:
            if now - self.last_sent_at >= CARD_INTERVAL:
                try:
                    await self.api.update_card(self.card_id, self.answer, False)
                    self.last_sent_at = now
                except Exception:
                    logger.exception("AI card update failed; using one final text reply")
                    self.card_id = ""

    async def finish(self) -> None:
        if self.card_id:
            try:
                await self.api.update_card(self.card_id, self.answer, True)
                return
            except Exception:
                logger.exception("AI card finalize failed; using one final text reply")
                self.card_id = ""
        await self.api.reply_text(self.message, self.answer)


class FastGPTHandler(dingtalk_stream.ChatbotHandler):
    """ACK promptly, deduplicate retries, and process each conversation in order."""

    def __init__(self, api: DingTalkApi):
        super().__init__()
        self.api = api
        self.tasks: set[asyncio.Task[None]] = set()
        self.seen: OrderedDict[str, float] = OrderedDict()
        self.locks: dict[str, asyncio.Lock] = {}

    async def process(self, callback: dingtalk_stream.CallbackMessage):
        message = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
        question = upstream.extract_text(message)
        if not question:
            return AckMessage.STATUS_OK, "OK"
        message_id = message.message_id
        now = time.monotonic()
        while self.seen and next(iter(self.seen.values())) < now - 600:
            self.seen.popitem(last=False)
        if message_id and message_id in self.seen:
            return AckMessage.STATUS_OK, "OK"
        if message_id:
            self.seen[message_id] = now
        sender_id = upstream.extract_sender_id(message)
        chat_id = upstream.extract_chat_id(message, sender_id)
        task = asyncio.create_task(self._run(message, question, chat_id))
        self.tasks.add(task)
        def on_done(done: asyncio.Task[None]) -> None:
            self.tasks.discard(done)
            if not done.cancelled() and (error := done.exception()):
                logger.error("message task failed: %s", error, exc_info=error)

        task.add_done_callback(on_done)
        return AckMessage.STATUS_OK, "OK"

    async def _run(self, message: dingtalk_stream.ChatbotMessage, question: str, chat_id: str) -> None:
        lock = self.locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            if upstream.is_reset_message(question):
                upstream.CHAT_ID_SUFFIXES[chat_id] = str(int(time.time()))
                await self.api.reply_text(message, "已重置对话上下文。")
                return
            if suffix := upstream.CHAT_ID_SUFFIXES.get(chat_id):
                chat_id = f"{chat_id}-{suffix}"
            logger.info("message received, chat=%s, message_id=%s", chat_id, message.message_id)
            await self._handle_answer(message, question, chat_id)

    async def _handle_answer(self, message: dingtalk_stream.ChatbotMessage, question: str, chat_id: str) -> None:
        try:
            await self.api.emotion(message, REACTION_EMOJI, False)
        except Exception:
            logger.exception("thinking reaction failed")
        card_id = ""
        if CARD_TEMPLATE_ID and CARD_ENABLED:
            try:
                card_id = await self.api.create_card(message)
            except Exception:
                logger.exception("AI card creation failed; using one final text reply")
        stream = ReplyStream(self.api, message, card_id)
        succeeded = False
        try:
            async for delta in stream_fastgpt(self.api.client, question, chat_id):
                await stream.add(delta)
            if not stream.answer.strip():
                raise RuntimeError("FastGPT returned an empty answer")
            await stream.finish()
            succeeded = True
            logger.info("reply complete, chat=%s, chars=%d, card=%s", chat_id, len(stream.answer), bool(card_id))
        except Exception:
            logger.exception("streamed reply failed")
            error_text = "FastGPT 暂时无法响应，请稍后重试。"
            try:
                if stream.card_id:
                    try:
                        await self.api.update_card(stream.card_id, stream.answer or error_text, True)
                    except Exception:
                        logger.exception("AI card error update failed; using text reply")
                        await self.api.reply_text(message, error_text)
                else:
                    await self.api.reply_text(message, error_text)
            except Exception:
                logger.exception("error reply failed")
        finally:
            try:
                await self.api.emotion(message, REACTION_EMOJI, True)
            except Exception:
                logger.exception("thinking reaction recall failed")
            if succeeded:
                try:
                    await self.api.emotion(message, DONE_EMOJI, False)
                except Exception:
                    logger.exception("done reaction failed")


def main() -> None:
    """Start the existing DingTalk Stream SDK with the enhanced callback handler."""
    if not all((upstream.DINGTALK_APP_KEY, upstream.DINGTALK_APP_SECRET, upstream.FASTGPT_BASE_URL,
                upstream.FASTGPT_API_KEY, upstream.FASTGPT_APP_ID)):
        raise SystemExit("DingTalk and FastGPT credentials are required")
    client = httpx.AsyncClient()
    stream_client = dingtalk_stream.DingTalkStreamClient(
        dingtalk_stream.Credential(upstream.DINGTALK_APP_KEY, upstream.DINGTALK_APP_SECRET)
    )
    stream_client.register_callback_handler(
        dingtalk_stream.chatbot.ChatbotMessage.TOPIC, FastGPTHandler(DingTalkApi(client))
    )
    logger.info("DingTalk Stream bridge started; AI card enabled=%s", bool(CARD_TEMPLATE_ID and CARD_ENABLED))
    stream_client.start_forever()


if __name__ == "__main__":
    main()
