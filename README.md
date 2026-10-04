# DingTalk Stream + FastGPT

单文件、无页面的钉钉 Stream 中间件：

```text
钉钉 Stream -> dingtalk_fastgpt.py -> FastGPT -> 钉钉
```

Stream 模式通过 WebSocket 长连接接收消息，不需要公网 IP、公网域名、HTTPS 回调地址或内网穿透。

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

配置 `.env`：

```dotenv
DINGTALK_APP_KEY=钉钉企业内部应用AppKey
DINGTALK_APP_SECRET=钉钉企业内部应用AppSecret
FASTGPT_BASE_URL=http://127.0.0.1:3000
FASTGPT_API_KEY=FastGPT应用APIKey
FASTGPT_APP_ID=FastGPT应用ID
FASTGPT_TIMEOUT=180
FASTGPT_CHAT_ID_MODE=user
FASTGPT_DETAIL=false
```

`FASTGPT_BASE_URL` 填 FastGPT 根地址，不要带 `/api/v1`。`FASTGPT_APP_ID` 必须填写目标应用的 ID，通常可以从 FastGPT 应用页面 URL 中找到。
`FASTGPT_CHAT_ID_MODE=user` 表示每个钉钉用户使用独立 FastGPT 上下文；群里多人共用上下文时才改成 `conversation`。
排查知识库命中问题时，可以临时设置 `FASTGPT_DETAIL=true`，中间件会打印 FastGPT 返回的调试摘要。

## 钉钉后台

1. 创建或打开钉钉企业内部应用。
2. 开启机器人能力。
3. 消息接收模式选择 **Stream 模式**。
4. 配置机器人收消息和发消息权限。
5. 发布应用版本。

不需要填写回调 URL。

## 启动

```bash
python3 dingtalk_fastgpt.py
```

看到下面的日志，就会开始等待钉钉消息：

```text
DingTalk Stream -> FastGPT middleware started.
无需公网地址；按 Ctrl+C 停止。
```

机器人收到问题后，中间件会调用：

```text
POST {FASTGPT_BASE_URL}/api/v1/chat/completions
```

并使用钉钉用户或会话 ID 作为 FastGPT `chatId`。
