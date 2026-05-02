# AI Gateway — 多應用 AI API 閘道器

多應用 AI API 閘道器，提供 API Key 池化管理、三層索引數據庫、每日 AI 彙整。

## 核心功能

- **API Key 池化** — 多個 API Key 輪詢調度，速率限制，自動排隊
- **三層索引 DB** — `app_id → user_id → local_path`，SQLite + WAL
- **多提供商支持** — NVIDIA NIM / OpenAI / Anthropic，可擴展
- **每日 AI 彙整** — 每日 23:55 自動彙整用戶原始數據
- **管理 Web UI** — `/admin` 可視化管理 Keys、應用、Sessions

## 快速開始

```bash
npm install
cp .env.example .env  # 編輯 .env 填入 API Key
npm start
```

## 環境變量

| 變量 | 默認值 | 說明 |
|------|--------|------|
| `PORT` | 3005 | 服務端口 |
| `AI_MODEL` | meta/llama-3.1-8b-instruct | 默認 AI 模型 |
| `AI_BASE_URL` | https://integrate.api.nvidia.com/v1 | 默認 API Base URL |
| `SUMMARY_MODEL` | meta/llama-3.3-70b-instruct | 彙整用模型 |
| `SUMMARY_TIME` | 23:55 | 每日彙整時間 |
| `NVIDIA_API_KEY` | — | 備用 API Key |

## API

- `POST /api/query` — AI 查詢（主要端點）
- `GET /api/health` — 健康檢查
- `GET /api/admin/keys` — 列出 API Keys
- `POST /api/admin/keys` — 新增 Key
- `PUT /api/admin/keys/:id` — 更新 Key
- `DELETE /api/admin/keys/:id` — 刪除 Key
- `GET /api/admin/apps` — 列出應用
- `GET /api/admin/sessions` — Session 歷史
- `POST /api/admin/trigger-summary` — 手動觸發彙整

## 客戶端接入

```javascript
const res = await fetch('https://www.herelai.fun/ws/05-ai-gateway/api/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    app_id: 'your_app',
    user_id: 'user_id',
    query_data: '用戶問題',
  })
});
const { response } = await res.json();
```

## 授權

MIT
