"""Focused tests for the DingTalk streaming bridge."""

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

import bridge


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_fastgpt_stream_excludes_reasoning(self):
        events = [
            {"choices": [{"delta": {"reasoning_content": "private thought"}}]},
            {"choices": [{"delta": {"content": "Hello"}}]},
            {"choices": [{"delta": {"content": " world"}}]},
        ]
        body = "".join(f"data: {json.dumps(event)}\n\n" for event in events) + "data: [DONE]\n\n"

        def respond(request):
            self.assertEqual(request.url.path, "/api/v1/chat/completions")
            self.assertTrue(json.loads(request.content)["stream"])
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=body)

        old_base = bridge.upstream.FASTGPT_BASE_URL
        bridge.upstream.FASTGPT_BASE_URL = "https://fastgpt.example"
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                chunks = [part async for part in bridge.stream_fastgpt(client, "question", "chat")]
        finally:
            bridge.upstream.FASTGPT_BASE_URL = old_base
        self.assertEqual(chunks, ["Hello", " world"])

    async def test_without_card_sends_one_complete_reply(self):
        api = SimpleNamespace(reply_text=AsyncMock())
        stream = bridge.ReplyStream(api, SimpleNamespace())
        await stream.add("a" * 50)
        await stream.add("b" * 4050)
        api.reply_text.assert_not_awaited()
        await stream.finish()
        api.reply_text.assert_awaited_once()
        self.assertEqual(api.reply_text.await_args.args[1], "a" * 50 + "b" * 4050)

    async def test_card_update_failure_falls_back_to_text(self):
        api = SimpleNamespace(update_card=AsyncMock(side_effect=RuntimeError("card failed")), reply_text=AsyncMock())
        stream = bridge.ReplyStream(api, SimpleNamespace(), "card-id")
        await stream.add("answer")
        await stream.finish()
        self.assertEqual(stream.card_id, "")
        api.reply_text.assert_awaited_once()
        self.assertEqual(api.reply_text.await_args.args[1], "answer")

    async def test_card_is_created_and_streamed_with_generated_track_id(self):
        client = httpx.AsyncClient()
        api = bridge.DingTalkApi(client)
        api.request = AsyncMock(return_value={"result": {"cardInstanceId": "instance", "outTrackId": "track"}})
        message = SimpleNamespace(
            conversation_type="1", sender_staff_id="staff", conversation_id="conversation", robot_code="robot"
        )
        try:
            with patch.object(bridge, "CARD_TEMPLATE_ID", "template"):
                track_id = await api.create_card(message)
                stream = bridge.ReplyStream(api, message, track_id)
                await stream.add("Hello")
                await stream.add(" world")
                await stream.finish()
        finally:
            await client.aclose()
        self.assertEqual(track_id, "track")
        create_call, update_call, final_call = api.request.await_args_list
        self.assertEqual(create_call.args[1], "/v1.0/card/instances/createAndDeliver")
        self.assertEqual(create_call.args[2]["cardTemplateId"], "template")
        self.assertTrue(create_call.args[2]["outTrackId"].startswith("fastgpt_"))
        self.assertEqual(create_call.args[2]["cardData"]["cardParamMap"]["content"], "正在处理...")
        self.assertEqual(create_call.args[2]["userId"], "staff")
        self.assertEqual(create_call.args[2]["openSpaceId"], "dtv1.card//im_robot.staff")
        self.assertEqual(update_call.args[1], "/v1.0/card/streaming")
        self.assertEqual(update_call.args[2]["outTrackId"], "track")
        self.assertEqual(final_call.args[2]["content"], "Hello world")
        self.assertTrue(final_call.args[2]["isFinalize"])

    async def test_markdown_reply_converts_table(self):
        sent = []

        def respond(request):
            sent.append(json.loads(request.content))
            return httpx.Response(200, json={"errcode": 0})

        message = SimpleNamespace(session_webhook="https://dingtalk.example/reply", sender_staff_id="staff")
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            await bridge.DingTalkApi(client).reply_text(
                message, "## Leave\n\n| Check | Why |\n|---|---|\n| Balance | Avoid rejection |"
            )
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["msgtype"], "markdown")
        self.assertIn("**Check：** Balance", sent[0]["markdown"]["text"])
        self.assertNotIn("|---|---|", sent[0]["markdown"]["text"])

    async def test_reaction_add_and_recall_target_original_message(self):
        api = bridge.DingTalkApi(httpx.AsyncClient())
        api.request = AsyncMock(return_value={})
        message = SimpleNamespace(message_id="msg", conversation_id="conv", robot_code="robot")
        try:
            await api.emotion(message, "Thinking", False)
            await api.emotion(message, "Thinking", True)
        finally:
            await api.client.aclose()
        calls = api.request.await_args_list
        self.assertEqual(calls[0].args[1], "/v1.0/robot/emotion/reply")
        self.assertEqual(calls[1].args[1], "/v1.0/robot/emotion/recall")
        self.assertEqual(calls[0].args[2]["openMsgId"], "msg")
        self.assertEqual(calls[1].args[2]["openConversationId"], "conv")


if __name__ == "__main__":
    unittest.main()
