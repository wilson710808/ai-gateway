# AI Gateway Server v2.0

多應用 AI API 閘道器，支持 API Key 池化、多 session 併發、三層索引數據庫及每日彙整。

## 核心功能

1. **三層索引數據庫** — `app_id → user_id → local_path`
2. **API Key 池化 v2** — 併發安全 + 滑動窗口速率限制 + 負載均衡
3. **每日 Raw Data 記錄 + 23:55 AI 彙整**
4. **管理 Web UI**

## v2 API Key Pool 機制

### 架構設計

| 機制 | v1 (舊) | v2 (新) |
|------|---------|---------|
| 併發模型 | 獨佔鎖定：1 key = 1 session | 併發槽位：1 key = N sessions |
| 速率限制 | 固定窗口（邊界突增問題） | 滑動窗口（精確追蹤最近 60s） |
| 分配策略 | 順序遍歷（第一個 key 負載最高） | 加權輪詢（選併發最低的 key） |
| 僵死處理 | 無（crash 後 key 永久鎖定） | 30s 定時掃描，超時自動回收 |
| 佇列調度 | release 只嘗試當前 key | release 後掃描所有可用 key |
| AI 調用失敗 | 仍需手動 release | finally 保證自動 release |

### 併發能力

- 每個 API Key 可同時服務 `max_concurrent`（預設 5）個 session
- 4 個 key × 5 併發 = **總併發容量 20**
- 速率限制：每 key 每分鐘 `rate_limit`（預設 10）次請求
- 超過併發/速率限制的 session 自動進入 FIFO 佇列等待
- 佇列滿時（預設 100）直接拒絕新請求

### 通信協議

```
客戶端 → POST /api/query
{
  "app_id": "your_app_id",
  "user_id": "user_identifier",
  "query_data": "用戶輸入文本",
  "messages": [                    // 可選：多輪對話歷史
    { "role": "user", "content": "之前說的..." },
    { "role": "assistant", "content": "之前的回覆..." },
    { "role": "user", "content": "當前問題" }
  ]
}

← 回應
{
  "success": true,
  "session_id": "sess_xxx_xxx_timestamp",
  "response": "AI 回覆內容",
  "local_path": "/path/to/user/data",
  "duration_ms": 1234,
  "context_used": true
}
```

## 環境配置

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `PORT` | 3005 | 服務端口 |
| `AI_BASE_URL` | NVIDIA NIM API | AI API 基礎 URL |
| `AI_MODEL` | meta/llama-3.1-8b-instruct | 預設模型 |
| `SUMMARY_MODEL` | meta/llama-3.3-70b-instruct | 彙整用模型 |
| `MAX_CONCURRENT_PER_KEY` | 5 | 每 key 最大併發數 |
| `KEY_STALE_TIMEOUT` | 120000 (2 min) | 僵死 session 回收超時 |
| `MAX_QUEUE_SIZE` | 100 | 佇列最大長度 |
| `SESSION_TIMEOUT` | 300000 (5 min) | 排隊超時 |

## API 端點

### 核心
- `POST /api/query` — AI 查詢
- `GET /api/health` — 健康檢查（含 key 池狀態）

### 管理
- `GET/POST/PUT/DELETE /api/admin/keys` — API Key CRUD
- `GET /api/admin/apps` — 應用列表
- `GET /api/admin/users` — 用戶列表
- `GET /api/admin/sessions` — Session 日誌
- `GET /api/admin/summaries` — 彙整記錄
- `GET /api/admin/stats` — 統計數據
- `POST /api/admin/trigger-summary` — 手動觸發彙整

## 部署

```bash
# 安裝依賴
npm install

# 啟動
node server.js
```

systemd 服務：`webspace-05-ai-gateway.service`
