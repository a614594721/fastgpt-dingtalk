"""DingTalk Stream -> FastGPT middleware.

This process uses DingTalk Stream mode, so it does not need a public URL.

Run:
    python3 dingtalk_fastgpt.py
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from pathlib import Path
from typing import Any

import httpx


def load_env() -> None:
    path = Path(".env")
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env()

DINGTALK_APP_KEY = os.getenv("DINGTALK_APP_KEY", "")
DINGTALK_APP_SECRET = os.getenv("DINGTALK_APP_SECRET", "")
FASTGPT_BASE_URL = os.getenv("FASTGPT_BASE_URL", "").rstrip("/")
FASTGPT_API_KEY = os.getenv("FASTGPT_API_KEY", "")
FASTGPT_APP_ID = os.getenv("FASTGPT_APP_ID", "")
FASTGPT_TIMEOUT = int(os.getenv("FASTGPT_TIMEOUT", "180"))
FASTGPT_DETAIL = os.getenv("FASTGPT_DETAIL", "false").lower() in {"1", "true", "yes", "on"}
FASTGPT_CHAT_ID_MODE = os.getenv("FASTGPT_CHAT_ID_MODE", "user").lower()
CHAT_ID_SUFFIXES: dict[str, str] = {}


def fastgpt_answer(question: str, chat_id: str) -> str:
    if not FASTGPT_BASE_URL or not FASTGPT_API_KEY or not FASTGPT_APP_ID:
        raise RuntimeError("请配置 FASTGPT_BASE_URL、FASTGPT_API_KEY 和 FASTGPT_APP_ID")

    payload: dict[str, Any] = {
        "appId": FASTGPT_APP_ID,
        "chatId": chat_id or "dingtalk-user",
        "stream": False,
        "detail": FASTGPT_DETAIL,
        "messages": [{"role": "user", "content": question}],
    }

    try:
        response = httpx.post(
            f"{FASTGPT_BASE_URL}/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {FASTGPT_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=FASTGPT_TIMEOUT,
        )
    except httpx.TimeoutException as exc:
        raise RuntimeError(f"FastGPT 请求超时（>{FASTGPT_TIMEOUT}s）") from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"FastGPT 网络请求失败：{exc}") from exc
    if response.status_code >= 400:
        try:
            detail = response.json()
        except ValueError:
            detail = response.text[:500]
        raise RuntimeError(f"FastGPT HTTP {response.status_code}: {detail}")
    response.raise_for_status()
    data = response.json()
    if FASTGPT_DETAIL:
        print(f"FastGPT detail: {_summarize_fastgpt_detail(data)}")
    content = data["choices"][0]["message"]["content"]
    if isinstance(content, list):
        content = "".join(
            item.get("text", "") if isinstance(item, dict) else str(item)
            for item in content
        )
    answer = str(content).strip()
    if not answer:
        raise RuntimeError("FastGPT 返回了空答案")
    return answer


def _summarize_fastgpt_detail(data: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key in ("responseData", "usage"):
        if key in data:
            summary[key] = data[key]
    return summary or {"keys": sorted(data.keys())}


def extract_text(message: object) -> str:
    text = getattr(message, "text", None)
    content = getattr(text, "content", "") if text else ""
    # In group chats DingTalk may include bot mentions in the text.
    return re.sub(r"@\S+", "", str(content or "")).strip()


def extract_sender_id(message: object) -> str:
    for name in ("sender_staff_id", "senderStaffId", "sender_id", "senderId"):
        value = getattr(message, name, "")
        if value:
            return str(value)
    raw = getattr(message, "raw", None)
    if isinstance(raw, dict):
        return str(raw.get("senderStaffId") or raw.get("senderId") or "")
    return ""


def extract_chat_id(message: object, sender_id: str) -> str:
    if FASTGPT_CHAT_ID_MODE == "user":
        return sender_id or "dingtalk-user"
    for name in ("conversation_id", "conversationId", "conversation_title", "conversationTitle"):
        value = getattr(message, name, "")
        if value:
            return str(value)
    raw = getattr(message, "raw", None)
    if isinstance(raw, dict):
        return str(raw.get("conversationId") or raw.get("conversationTitle") or sender_id)
    return sender_id or "dingtalk-user"


def is_reset_message(question: str) -> bool:
    return question.strip().lower() in {"reset", "/reset", "重置", "清空上下文", "重新开始"}


async def handle_message(handler: object, incoming_message: object) -> None:
    question = extract_text(incoming_message)
    if not question:
        return

    sender_id = extract_sender_id(incoming_message)
    base_chat_id = extract_chat_id(incoming_message, sender_id)
    if is_reset_message(question):
        CHAT_ID_SUFFIXES[base_chat_id] = str(int(time.time()))
        try:
            handler.reply_text("已重置，本次之后会使用新的对话上下文。", incoming_message)
        except Exception as exc:
            print(f"DingTalk reset reply failed: {exc}")
        return
    chat_id = base_chat_id
    if base_chat_id in CHAT_ID_SUFFIXES:
        chat_id = f"{base_chat_id}-{CHAT_ID_SUFFIXES[base_chat_id]}"
    print(f"received message sender={sender_id or '-'} chat={chat_id}: {question}")

    try:
        handler.reply_text("正在查询知识库，请稍等...", incoming_message)
    except Exception as exc:
        print(f"DingTalk progress reply failed: {exc}")

    try:
        answer = await asyncio.to_thread(fastgpt_answer, question, chat_id)
    except Exception as exc:
        print(f"FastGPT request failed: {exc}")
        answer = "FastGPT 暂时无法响应，请稍后再试。"

    try:
        handler.reply_text(answer[:4500], incoming_message)
    except Exception as exc:
        print(f"DingTalk reply failed: {exc}")


def main() -> None:
    if not DINGTALK_APP_KEY or not DINGTALK_APP_SECRET:
        raise SystemExit("请先在 .env 配置 DINGTALK_APP_KEY 和 DINGTALK_APP_SECRET")
    if not FASTGPT_BASE_URL or not FASTGPT_API_KEY or not FASTGPT_APP_ID:
        raise SystemExit("请先在 .env 配置 FASTGPT_BASE_URL、FASTGPT_API_KEY 和 FASTGPT_APP_ID")

    try:
        import dingtalk_stream
        from dingtalk_stream import AckMessage
    except ImportError as exc:
        raise SystemExit("请先执行：pip install -r requirements.txt") from exc

    class FastGPTHandler(dingtalk_stream.ChatbotHandler):
        async def process(self, callback: dingtalk_stream.CallbackMessage):
            incoming_message = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
            await handle_message(self, incoming_message)
            return AckMessage.STATUS_OK, "OK"

    credential = dingtalk_stream.Credential(DINGTALK_APP_KEY, DINGTALK_APP_SECRET)
    client = dingtalk_stream.DingTalkStreamClient(credential)
    client.register_callback_handler(
        dingtalk_stream.chatbot.ChatbotMessage.TOPIC,
        FastGPTHandler(),
    )
    print("DingTalk Stream -> FastGPT middleware started.")
    print("无需公网地址；按 Ctrl+C 停止。")
    client.start_forever()


if __name__ == "__main__":
    main()
