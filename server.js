/**
 * AI Gateway Server (HTTPS) — 多應用 AI API 閘道器
 * 
 * 核心功能：
 * 1. 三層索引數據庫 (app_id → user_id → local_path)
 * 2. TCP/IP Session 管理 + API Key 池化 v2（併發安全 + 速率限制）
 * 3. 每日 Raw Data 記錄 + 23:55 AI 彙整
 * 4. 管理 Web UI
 * 
 * v2 API Key Pool 改進：
 * - 取消獨佔鎖定，同一 key 支持多 session 併發
 * - 滑動窗口速率限制取代固定窗口
 * - 加權輪詢：優先分配併發數最低的 key
 * - 僵死 session 自動回收機制
 * - Queue 公平調度：release 時嘗試喚醒所有等待者
 */
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const Database = require('better-sqlite3');

// ============================================
// 配置
// ============================================
const CONFIG = {
  port: parseInt(process.env.PORT || '3005'),
  dataDir: process.env.DATA_DIR || path.join(__dirname, 'data'),
  dbPath: process.env.DB_PATH || path.join(__dirname, 'ai-gateway.db'),
  defaultAIModel: process.env.AI_MODEL || 'meta/llama-3.1-8b-instruct',
  defaultAIBaseUrl: process.env.AI_BASE_URL || 'https://integrate.api.nvidia.com/v1',
  summaryTime: process.env.SUMMARY_TIME || '23:55', // 每日彙整觸發時間
  summaryModel: process.env.SUMMARY_MODEL || 'meta/llama-3.3-70b-instruct', // 彙整用更強模型
  maxQueueSize: parseInt(process.env.MAX_QUEUE_SIZE || '100'),
  sessionTimeout: parseInt(process.env.SESSION_TIMEOUT || '300000'), // 5 分鐘
  maxConcurrentPerKey: parseInt(process.env.MAX_CONCURRENT_PER_KEY || '5'), // 每 key 最大併發數
  keyStaleTimeout: parseInt(process.env.KEY_STALE_TIMEOUT || '120000'), // 2 分鐘：僵死 session 自動釋放
  // 安全配置
  adminToken: process.env.ADMIN_TOKEN || crypto.randomBytes(32).toString('hex'),
  corsOrigins: process.env.CORS_ORIGINS || '', // 留空則同源限制，'*' 則全開
  sessionArchiveDays: parseInt(process.env.SESSION_ARCHIVE_DAYS || '7'), // session 保留天數
};

// 確保數據目錄存在
if (!fs.existsSync(CONFIG.dataDir)) fs.mkdirSync(CONFIG.dataDir, { recursive: true });

// ============================================
// 數據庫 — 三層索引
// ============================================
const db = new Database(CONFIG.dbPath);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// 創建表
db.exec(`
  -- 第一層：應用 (app_id)
  CREATE TABLE IF NOT EXISTS apps (
    app_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  -- 第二層：用戶 (user_id) + 第三層：local_path
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    local_path TEXT,
    metadata TEXT DEFAULT '{}',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_active DATETIME,
    FOREIGN KEY (app_id) REFERENCES apps(app_id) ON DELETE CASCADE,
    UNIQUE(app_id, user_id)
  );

  -- API Key 池
  CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_value TEXT NOT NULL UNIQUE,
    label TEXT,
    base_url TEXT DEFAULT '${CONFIG.defaultAIBaseUrl}',
    model TEXT DEFAULT '${CONFIG.defaultAIModel}',
    is_active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_used DATETIME,
    total_calls INTEGER DEFAULT 0,
    rate_limit INTEGER DEFAULT 10, -- 每分鐘最大請求數
    max_concurrent INTEGER DEFAULT 5  -- 每 key 最大併發數
  );

  -- Session 日誌
  CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL UNIQUE,
    app_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    api_key_id INTEGER,
    status TEXT DEFAULT 'queued' CHECK(status IN ('queued', 'active', 'completed', 'failed', 'timeout')),
    query_preview TEXT,
    response_preview TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    duration_ms INTEGER,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
  );

  -- 彙整記錄
  CREATE TABLE IF NOT EXISTS summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    week_start DATE NOT NULL,
    week_end DATE NOT NULL,
    summary_text TEXT,
    raw_data_file TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    UNIQUE(app_id, user_id, week_start)
  );

  -- 索引
  CREATE INDEX IF NOT EXISTS idx_users_app ON users(app_id);
  CREATE INDEX IF NOT EXISTS idx_users_app_user ON users(app_id, user_id);
  CREATE INDEX IF NOT EXISTS idx_sessions_app_user ON sessions(app_id, user_id);
  CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
  CREATE INDEX IF NOT EXISTS idx_sessions_created ON sessions(created_at);
  CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active);
  CREATE INDEX IF NOT EXISTS idx_summaries_lookup ON summaries(app_id, user_id, week_start);
`);

// ============================================
// API Key 池化管理器 v2 — 併發安全 + 速率限制
//
// 核心設計：
// 1. 取消獨佔鎖定：一個 key 可同時服務多個 session（maxConcurrent 控制）
// 2. 滑動窗口速率限制：精確追蹤最近 60s 內的請求數，避免固定窗口邊界突增
// 3. 加權輪詢分配：優先選擇當前併發數最低的 key，實現負載均衡
// 4. 僵死 session 回收：超時未 release 的 session 自動釋放併發槽位
// 5. 佇列公平調度：FIFO + release 時嘗試喚醒所有可能匹配的等待者
// 6. 加權輪詢均衡：結合即時併發 + 歷史累計 + round-robin 指標，確保 key 平均使用
class APIKeyPool {
  constructor() {
    this.keys = [];
    // 每個 key 的併發追蹤：keyId → Set<{ sessionId, acquiredAt }>
    this.activeSessions = new Map();
    // 滑動窗口速率計數：keyId → [timestamp, ...]（最近 60s 內的請求時間戳）
    this.rateWindows = new Map();
    // 等待佇列
    this.queue = []; // { sessionId, resolve, reject, timeout, enqueuedAt }
    // Round-Robin 指標：每次 acquire 後推進，確保低頻時均勻輪換
    this._rrIndex = 0;
    // Runtime 累計計數器（重啟後從 DB total_calls 同步）
    this._runtimeCalls = new Map(); // keyId → number
    // 定時清理僵死 session
    this._staleCheckInterval = setInterval(() => this._reapStaleSessions(), 30000);
  }

  /** 從 DB 載入活躍 key，同步歷史累計到 runtime 計數器 */
  _loadKeys() {
    const rows = db.prepare('SELECT * FROM api_keys WHERE is_active = 1').all();
    this.keys = rows;
    // 確保每個 key 的追蹤結構存在
    for (const key of this.keys) {
      if (!this.activeSessions.has(key.id)) {
        this.activeSessions.set(key.id, new Set());
      }
      if (!this.rateWindows.has(key.id)) {
        this.rateWindows.set(key.id, new RateWindow(this._getRateLimit(key)));
      }
      // 同步 DB total_calls 到 runtime 計數器（重啟後保留歷史）
      if (!this._runtimeCalls.has(key.id)) {
        this._runtimeCalls.set(key.id, key.total_calls || 0);
      }
    }
    // 清理已刪除 key 的追蹤
    for (const keyId of [...this.activeSessions.keys()]) {
      if (!this.keys.some(k => k.id === keyId)) {
        this.activeSessions.delete(keyId);
        this.rateWindows.delete(keyId);
        this._runtimeCalls.delete(keyId);
      }
    }
    // 修正 _rrIndex 避免越界
    if (this._rrIndex >= this.keys.length) this._rrIndex = 0;

    const totalConcurrentCap = rows.reduce((s, k) => s + this._getMaxConcurrent(k), 0);
    console.log(`[APIKeyPool] 載入 ${rows.length} 個活躍 API Key (總併發容量: ${totalConcurrentCap})`);
    rows.forEach(k => {
      console.log(`  Key#${k.id} (${k.label}): total_calls=${k.total_calls}, max_concurrent=${this._getMaxConcurrent(k)}`);
    });
  }

  reload() {
    this._loadKeys();
    // reload 後嘗試喚醒佇列（可能新增了 key）
    this._drainQueue();
  }

  /** 獲取 key 的最大併發數 */
  _getMaxConcurrent(key) {
    return key.max_concurrent || CONFIG.maxConcurrentPerKey;
  }

  /** 獲取 key 的速率限制（每分鐘） */
  _getRateLimit(key) {
    return key.rate_limit || 10;
  }

  /** 滑動窗口速率檢查：最近 60s 內的請求數是否超限 */
  _checkRateLimit(keyId) {
    const window = this.rateWindows.get(keyId);
    if (!window) return true;
    window.prune();
    const key = this.keys.find(k => k.id === keyId);
    return window.recentCount() < this._getRateLimit(key);
  }

  /** 記錄一次速率窗口請求 */
  _recordRateHit(keyId) {
    const window = this.rateWindows.get(keyId);
    if (window) window.push(Date.now());
  }

  /**
   * 選擇最佳 key：三維加權評分
   *
   * 評分維度：
   *   1. 即時併發（權重 50%）— 併發數越低分越高
   *   2. 歷史累計調用（權重 30%）— 調用次數越少分越高
   *   3. Round-Robin 輪次（權重 20%）— 距上次分配越遠分越高
   *
   * 結果：高頻時偏向低併發 key，低頻時確保均勻輪換，
   *       長期來看各 key 的 total_calls 會趨於平均
   */
  _selectBestKey() {
    const candidates = this.keys.filter(key => {
      const concurrentCount = this.activeSessions.get(key.id)?.size || 0;
      if (concurrentCount >= this._getMaxConcurrent(key)) return false;
      if (!this._checkRateLimit(key.id)) return false;
      return true;
    });

    if (candidates.length === 0) return null;
    if (candidates.length === 1) return candidates[0];

    // 計算各維度的最大值用於歸一化
    const maxConcurrent = Math.max(...candidates.map(k => (this.activeSessions.get(k.id)?.size || 0)), 1);
    const maxTotalCalls = Math.max(...candidates.map(k => this._runtimeCalls.get(k.id) || 0), 1);

    let bestKey = null;
    let bestScore = -1;

    for (const key of candidates) {
      const concurrentCount = this.activeSessions.get(key.id)?.size || 0;
      const totalCalls = this._runtimeCalls.get(key.id) || 0;
      const keyIndex = this.keys.indexOf(key);

      // 1. 併發分數 (0~1)：併發越低分越高
      const concurrentScore = 1 - (concurrentCount / maxConcurrent);

      // 2. 歷史調用分數 (0~1)：調用越少分越高
      const callScore = 1 - (totalCalls / maxTotalCalls);

      // 3. Round-Robin 分數 (0~1)：距離 _rrIndex 越遠分越高
      //    下一位 = _rrIndex 本身得 1.0，其餘按距離遞減
      const distance = (keyIndex - this._rrIndex + this.keys.length) % this.keys.length;
      const rrScore = distance === 0 ? 1.0 : Math.max(0, 1 - (distance / this.keys.length));

      // 加權綜合分數
      const score = (concurrentScore * 0.5) + (callScore * 0.3) + (rrScore * 0.2);

      if (score > bestScore) {
        bestScore = score;
        bestKey = key;
      }
    }

    return bestKey;
  }

  /**
   * 獲取一個可用的 API Key
   * @param {string} sessionId - 當前 session ID
   * @returns {Promise<Object>} - 返回可用的 key 物件
   */
  async acquire(sessionId) {
    const key = this._selectBestKey();
    if (key) {
      return this._assignKey(key, sessionId);
    }

    // 無可用 key → 加入佇列等待
    if (this.queue.length >= CONFIG.maxQueueSize) {
      throw new Error('Queue full — too many concurrent requests');
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const idx = this.queue.findIndex(q => q.sessionId === sessionId);
        if (idx !== -1) this.queue.splice(idx, 1);
        reject(new Error('Session queued timeout'));
      }, CONFIG.sessionTimeout);

      this.queue.push({ sessionId, resolve, reject, timeout, enqueuedAt: Date.now() });
      console.log(`[APIKeyPool] Session ${sessionId} 排隊中 (佇列長度: ${this.queue.length})`);
    });
  }

  /** 分配 key 給 session（內部方法） */
  _assignKey(key, sessionId) {
    // 記錄併發追蹤
    if (!this.activeSessions.has(key.id)) {
      this.activeSessions.set(key.id, new Set());
    }
    this.activeSessions.get(key.id).add({ sessionId, acquiredAt: Date.now() });

    // 記錄速率窗口
    this._recordRateHit(key.id);

    // 更新 runtime 累計計數器
    this._runtimeCalls.set(key.id, (this._runtimeCalls.get(key.id) || 0) + 1);

    // 更新 DB 統計
    db.prepare('UPDATE api_keys SET last_used = ?, total_calls = total_calls + 1 WHERE id = ?')
      .run(new Date().toISOString(), key.id);

    // 推進 Round-Robin 指標到下一個 key
    const keyIndex = this.keys.indexOf(key);
    this._rrIndex = (keyIndex + 1) % this.keys.length;

    const concurrentCount = this.activeSessions.get(key.id).size;
    const maxConcurrent = this._getMaxConcurrent(key);
    const runtimeCalls = this._runtimeCalls.get(key.id);
    console.log(`[APIKeyPool] Key#${key.id} (${key.label}) 分配給 ${sessionId} (併發: ${concurrentCount}/${maxConcurrent}, 累計: ${runtimeCalls})`);
    return key;
  }

  /**
   * 釋放 key 的併發槽位
   * @param {number} keyId - API Key ID
   * @param {string} sessionId - 要釋放的 session ID
   */
  release(keyId, sessionId) {
    const active = this.activeSessions.get(keyId);
    if (active) {
      // 找到並移除對應 session 的追蹤記錄
      for (const entry of active) {
        if (entry.sessionId === sessionId) {
          active.delete(entry);
          break;
        }
      }
      const concurrentCount = active.size;
      const key = this.keys.find(k => k.id === keyId);
      console.log(`[APIKeyPool] Key#${keyId} 釋放 ${sessionId} (剩餘併發: ${concurrentCount}/${this._getMaxConcurrent(key)})`);
    }

    // 嘗試喚醒佇列中等待的 session
    this._drainQueue();
  }

  /** 從佇列中按 FIFO 分配可用 key */
  _drainQueue() {
    while (this.queue.length > 0) {
      const key = this._selectBestKey();
      if (!key) break; // 沒有可用 key 了

      const next = this.queue.shift();
      clearTimeout(next.timeout);

      try {
        const assignedKey = this._assignKey(key, next.sessionId);
        next.resolve(assignedKey);
      } catch (err) {
        next.reject(err);
      }
    }
  }

  /** 回收僵死 session（超時未 release） */
  _reapStaleSessions() {
    const now = Date.now();
    let reapedCount = 0;

    for (const [keyId, active] of this.activeSessions) {
      const staleEntries = [];
      for (const entry of active) {
        if (now - entry.acquiredAt > CONFIG.keyStaleTimeout) {
          staleEntries.push(entry);
        }
      }
      for (const entry of staleEntries) {
        active.delete(entry);
        reapedCount++;
        console.warn(`[APIKeyPool] 回收僵死 session: ${entry.sessionId} (Key#${keyId}, 已佔用 ${Math.round((now - entry.acquiredAt) / 1000)}s)`);
      }
    }

    if (reapedCount > 0) {
      console.log(`[APIKeyPool] 回收了 ${reapedCount} 個僵死 session`);
      this._drainQueue();
    }
  }

  /** 取得池狀態（供 health/stats API 使用） */
  getStatus() {
    const keyDetails = this.keys.map(k => {
      const active = this.activeSessions.get(k.id);
      const concurrentCount = active ? active.size : 0;
      const maxConcurrent = this._getMaxConcurrent(k);
      const rateWindow = this.rateWindows.get(k.id) || [];
      const recentHits = rateWindow ? rateWindow.recentCount() : 0;
      const runtimeCalls = this._runtimeCalls.get(k.id) || 0;
      return {
        id: k.id,
        label: k.label,
        concurrent: concurrentCount,
        maxConcurrent,
        rateLimit: this._getRateLimit(k),
        rateUsed: recentHits,
        totalCalls: k.total_calls,
        runtimeCalls,
        model: k.model,
      };
    });

    return {
      totalKeys: this.keys.length,
      totalConcurrent: keyDetails.reduce((sum, k) => sum + k.concurrent, 0),
      maxTotalConcurrent: keyDetails.reduce((sum, k) => sum + k.maxConcurrent, 0),
      queueLength: this.queue.length,
      rrIndex: this._rrIndex,
      keys: keyDetails,
    };
  }

  /** 銷毀時清理定時器 */
  destroy() {
    clearInterval(this._staleCheckInterval);
  }
}

// 啟動時清理過期 sessions
if (CONFIG.sessionArchiveDays > 0) {
  const deleted = db.prepare(
    "DELETE FROM sessions WHERE created_at < datetime('now', '-' || ? || ' days')"
  ).run(CONFIG.sessionArchiveDays);
  if (deleted.changes > 0) {
    console.log(`[DB] 清理了 ${deleted.changes} 筆超過 ${CONFIG.sessionArchiveDays} 天的 session 記錄`);
  }
}

// ============================================
// 工具函數
// ============================================

/**
 * 校驗 app_id / user_id 格式
 * 只允許：字母、數字、連字符、底線、點
 * 防止路徑遍歷（../）、SQL 注入殘留、特殊字符
 */
function validateId(name, value) {
  if (!value || typeof value !== 'string') return `${name} 必須為非空字串`;
  if (value.length > 128) return `${name} 長度不得超過 128 字元`;
  if (!/^[a-zA-Z0-9._-]+$/.test(value)) return `${name} 只允許字母、數字、連字符、底線、點`;
  if (value.includes('..')) return `${name} 不得包含路徑遍歷字符`;
  return null;
}

// ============================================
// RateWindow — O(log n) 滑動窗口（二分清理）
// 取代 Array filter，避免高頻時 O(n) GC 壓力
// ============================================
class RateWindow {
  constructor(limit) {
    this.limit = limit;
    this.timestamps = [];
    this._maxSize = limit * 3;
  }
  push(ts) {
    this.timestamps.push(ts);
    if (this.timestamps.length > this._maxSize) this.prune();
  }
  prune() {
    const cutoff = Date.now() - 60000;
    let lo = 0, hi = this.timestamps.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.timestamps[mid] < cutoff) lo = mid + 1;
      else hi = mid;
    }
    if (lo > 0) this.timestamps = this.timestamps.slice(lo);
  }
  recentCount() {
    this.prune();
    return this.timestamps.length;
  }
}

const keyPool = new APIKeyPool();
// 初始載入
keyPool._loadKeys();

// ============================================
// 用戶數據路徑管理
// ============================================
function getOrCreateUserPath(appId, userId) {
  // 查找現有用戶
  const existing = db.prepare('SELECT local_path FROM users WHERE app_id = ? AND user_id = ?')
    .get(appId, userId);

  if (existing && existing.local_path) {
    // 確保目錄存在
    if (!fs.existsSync(existing.local_path)) {
      fs.mkdirSync(existing.local_path, { recursive: true });
    }
    return existing.local_path;
  }

  // 創建新路徑: data/{app_id}/{user_id}/
  const userPath = path.join(CONFIG.dataDir, appId, userId);
  fs.mkdirSync(userPath, { recursive: true });

  // 確保 app 存在
  db.prepare('INSERT OR IGNORE INTO apps (app_id, name) VALUES (?, ?)')
    .run(appId, appId);

  // 更新或創建用戶記錄
  db.prepare(`
    INSERT INTO users (app_id, user_id, local_path, last_active)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(app_id, user_id) DO UPDATE SET
      local_path = excluded.local_path,
      last_active = excluded.last_active
  `).run(appId, userId, userPath, new Date().toISOString());

  console.log(`[UserPath] 創建/更新: ${userPath}`);
  return userPath;
}

// ============================================
// Raw Data 記錄器
// ============================================
class RawDataLogger {
  constructor() {
    this.buffers = new Map(); // `${appId}_${userId}` → { lines[], date }
  }

  log(appId, userId, queryData) {
    const key = `${appId}\x00${userId}`;
    const today = new Date().toISOString().split('T')[0];

    if (!this.buffers.has(key)) {
      this.buffers.set(key, { lines: [], date: today });
    }

    const buf = this.buffers.get(key);

    // 如果跨天了，先 flush 舊的
    if (buf.date !== today) {
      this._flushToFile(key, buf);
      this.buffers.set(key, { lines: [], date: today });
    }

    const entry = {
      ts: new Date().toISOString(),
      query: queryData,
    };
    this.buffers.get(key).lines.push(JSON.stringify(entry));
  }

  _flushToFile(key, buf) {
    const [appId, userId] = key.split('\x00');
    const userPath = getOrCreateUserPath(appId, userId);
    const rawDir = path.join(userPath, 'raw');
    if (!fs.existsSync(rawDir)) fs.mkdirSync(rawDir, { recursive: true });

    const filename = `raw_${buf.date}.txt`;
    const filePath = path.join(rawDir, filename);
    fs.appendFileSync(filePath, buf.lines.join('\n') + '\n');
    console.log(`[RawData] 寫入 ${filePath} (${buf.lines.length} 筆)`);
  }

  flushAll() {
    for (const [key, buf] of this.buffers) {
      if (buf.lines.length > 0) {
        this._flushToFile(key, buf);
      }
    }
    this.buffers.clear();
  }

  getRawFile(appId, userId, date) {
    const userPath = getOrCreateUserPath(appId, userId);
    const filePath = path.join(userPath, 'raw', `raw_${date}.txt`);
    if (fs.existsSync(filePath)) {
      return fs.readFileSync(filePath, 'utf-8');
    }
    return null;
  }
}

const rawDataLogger = new RawDataLogger();

// ============================================
// AI 服務調用
// ============================================
async function callAI(apiKey, queryData, contextHistory = '', explicitMessages = null) {
  const baseUrl = apiKey.base_url || CONFIG.defaultAIBaseUrl;
  const model = apiKey.model || CONFIG.defaultAIModel;

  const messages = [];

  // 如果有背景彙整，加入系統提示
  if (contextHistory) {
    messages.push({
      role: 'system',
      content: `以下是該用戶的過往互動彙整，作為背景參考：\n${contextHistory}`
    });
  }

  // 如果客戶端直接傳入 messages 陣列（多輪對話），優先使用
  if (explicitMessages && Array.isArray(explicitMessages) && explicitMessages.length > 0) {
    messages.push(...explicitMessages);
  } else {
    messages.push({ role: 'user', content: queryData });
  }

  // 30s 超時控制，防止 AI API 無回應佔住併發槽位
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000);

  try {
  const response = await fetch(`${baseUrl}/chat/completions`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey.key_value}`,
    },
    body: JSON.stringify({
      model,
      messages,
      temperature: 0.7,
      max_tokens: 2000,
    }),
    signal: controller.signal,
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`AI API error ${response.status}: ${errText}`);
  }

  const data = await response.json();
  if (!data.choices || !data.choices[0] || !data.choices[0].message) {
    throw new Error('AI API 返回格式異常：缺少 choices[0].message');
  }
  return data.choices[0].message.content;
  } finally {
    clearTimeout(timeoutId);
  }
}

// ============================================
// 每日彙整 (23:55 Cron)
// ============================================
async function runDailySummary() {
  console.log('[Summary] 開始每日彙整...');

  // flush 所有 raw data
  rawDataLogger.flushAll();

  // 取得所有用戶
  const users = db.prepare('SELECT app_id, user_id, local_path FROM users').all();

  const today = new Date();
  const weekStart = getWeekStart(today);
  const weekEnd = getWeekEnd(today);

  for (const user of users) {
    try {
      // 收集本週的 raw data
      const weekRawData = collectWeekRawData(user.local_path, weekStart, weekEnd);
      if (!weekRawData || weekRawData.trim().length === 0) {
        console.log(`[Summary] ${user.app_id}/${user.user_id}: 無本週數據，跳過`);
        continue;
      }

      // 使用彙整專用 API Key（選併發最低的）
      const bestKey = keyPool._selectBestKey() || keyPool.keys[0];
      if (!bestKey) {
        console.log('[Summary] 無可用 API Key，跳過彙整');
        break;
      }

      const summaryPrompt = `請根據以下用戶本週的互動記錄，進行彙整摘要。
提取關鍵話題、偏好、需求模式，以便未來互動時作為背景參考。
以繁體中文回答，格式化為結構化摘要。

本週互動記錄：
${weekRawData.substring(0, 8000)}

請輸出摘要（包含：主要話題、用戶偏好、需求模式、重要結論）`;

      const summaryBaseUrl = CONFIG.defaultAIBaseUrl;
      const response = await fetch(`${summaryBaseUrl}/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${bestKey.key_value}`,
        },
        body: JSON.stringify({
          model: CONFIG.summaryModel,
          messages: [
            { role: 'system', content: '你是一位數據分析師，擅長從用戶互動記錄中提取關鍵洞察。' },
            { role: 'user', content: summaryPrompt }
          ],
          temperature: 0.3,
          max_tokens: 2000,
        }),
      });

      if (!response.ok) {
        console.error(`[Summary] AI 調用失敗: ${response.status}`);
        continue;
      }

      const data = await response.json();
      const summaryText = data.choices[0].message.content;

      // 存入數據庫
      db.prepare(`
        INSERT INTO summaries (app_id, user_id, week_start, week_end, summary_text)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(app_id, user_id, week_start) DO UPDATE SET
          summary_text = excluded.summary_text
      `).run(user.app_id, user.user_id, weekStart, weekEnd, summaryText);

      // 也存入本地文件
      const summaryDir = path.join(user.local_path, 'summaries');
      if (!fs.existsSync(summaryDir)) fs.mkdirSync(summaryDir, { recursive: true });
      fs.writeFileSync(path.join(summaryDir, `summary_${weekStart}.md`), summaryText);

      console.log(`[Summary] ${user.app_id}/${user.user_id}: 彙整完成`);
    } catch (err) {
      console.error(`[Summary] ${user.app_id}/${user.user_id} 失敗:`, err.message);
    }
  }

  console.log('[Summary] 每日彙整完成');
}

function getWeekStart(date) {
  const d = new Date(date);
  const day = d.getDay();
  const diff = d.getDate() - day + (day === 0 ? -6 : 1);
  const monday = new Date(d.setDate(diff));
  return monday.toISOString().split('T')[0];
}

function getWeekEnd(date) {
  const d = new Date(date);
  const day = d.getDay();
  const diff = d.getDate() - day + (day === 0 ? 0 : 7);
  const sunday = new Date(d.setDate(diff));
  return sunday.toISOString().split('T')[0];
}

function collectWeekRawData(localPath, weekStart, weekEnd) {
  const rawDir = path.join(localPath, 'raw');
  if (!fs.existsSync(rawDir)) return null;

  const files = fs.readdirSync(rawDir)
    .filter(f => f.startsWith('raw_') && f.endsWith('.txt'))
    .sort();

  let allData = '';
  for (const f of files) {
    const dateStr = f.replace('raw_', '').replace('.txt', '');
    if (dateStr >= weekStart && dateStr <= weekEnd) {
      allData += fs.readFileSync(path.join(rawDir, f), 'utf-8') + '\n';
    }
  }
  return allData;
}

function getUserContextHistory(appId, userId) {
  // 讀取最近 4 週的彙整作為背景
  const summaries = db.prepare(`
    SELECT summary_text FROM summaries
    WHERE app_id = ? AND user_id = ?
    ORDER BY week_start DESC
    LIMIT 4
  `).all(appId, userId);

  if (summaries.length === 0) return '';
  return summaries.map((s, i) => `=== 第 ${summaries.length - i} 週彙整 ===\n${s.summary_text}`).join('\n\n');
}

// ============================================
// Express 應用
// ============================================
const app = express();

// CORS 配置：白名單制
const corsOptions = {
  origin: (origin, callback) => {
    if (!origin) return callback(null, true);
    if (CONFIG.corsOrigins === '*') return callback(null, true);
    if (!CONFIG.corsOrigins) return callback(null, false);
    const allowed = CONFIG.corsOrigins.split(',').map(s => s.trim());
    if (allowed.includes(origin)) return callback(null, true);
    callback(new Error('CORS not allowed'));
  },
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
};
app.use(require('cors')(corsOptions));

app.use(express.json({ limit: '1mb' }));

// 安全標頭
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.removeHeader('X-Powered-By');
  next();
});

// 簡易 IP 速率限制（每分鐘 60 次）
const ipRateLimits = new Map();
app.use((req, res, next) => {
  if (req.path.startsWith('/admin') || req.path === '/api/health') return next();
  const ip = req.ip || req.connection.remoteAddress;
  const now = Date.now();
  let entry = ipRateLimits.get(ip);
  if (!entry || now - entry.resetAt > 60000) {
    entry = { count: 0, resetAt: now };
    ipRateLimits.set(ip, entry);
  }
  entry.count++;
  if (entry.count > 60) {
    return res.status(429).json({ success: false, error: 'Rate limit exceeded (60/min)' });
  }
  next();
});
setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of ipRateLimits) {
    if (now - entry.resetAt > 120000) ipRateLimits.delete(ip);
  }
}, 5 * 60 * 1000);

// Admin 認證中間件
function adminAuth(req, res, next) {
  const authHeader = req.headers['authorization'];
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: '需要 Admin Token (Bearer Authorization header)' });
  }
  const token = authHeader.substring(7);
  if (token !== CONFIG.adminToken) {
    return res.status(403).json({ error: 'Admin Token 無效' });
  }
  next();
}

// 根路徑重定向到管理介面
app.get('/', (req, res) => {
  const prefix = (req.headers['x-forwarded-prefix'] || '').replace(/\/$/, '');
  res.redirect(prefix + '/admin/');
});

// ---- 核心 API ----

/**
 * GET /api/query — diagnostic endpoint for browser/health-check access.
 * The real AI query endpoint is POST /api/query.
 */
app.get('/api/query', (req, res) => {
  res.json({
    success: true,
    endpoint: '/api/query',
    method: 'GET',
    message: 'AI Gateway is reachable. Use POST /api/query for AI queries.',
    timestamp: new Date().toISOString()
  });
});

/**
 * POST /api/query — 主要 AI 查詢端點
 * Body: { app_id, user_id, query_data, options? }
 * Response: { success, session_id, response, local_path }
 */
app.post('/api/query', async (req, res) => {
  const { app_id, user_id, query_data, options, messages: reqMessages } = req.body;

  if (!app_id || !user_id) {
    return res.status(400).json({
      success: false,
      error: '缺少必要欄位: app_id, user_id'
    });
  }

  const appErr = validateId('app_id', app_id);
  if (appErr) return res.status(400).json({ success: false, error: appErr });

  const userErr = validateId('user_id', user_id);
  if (userErr) return res.status(400).json({ success: false, error: userErr });

  if (!query_data && (!reqMessages || reqMessages.length === 0)) {
    return res.status(400).json({
      success: false,
      error: '缺少 query_data 或 messages'
    });
  }

  // 兼容 messages-only 客戶端：以前 query_data 缺失時會在 substring() 直接拋錯，
  // Express 請求因此卡到客戶端 timeout，造成各 App 看起來像 AI Gateway 無回覆。
  const queryText = typeof query_data === 'string' && query_data.trim()
    ? query_data
    : (Array.isArray(reqMessages)
        ? (reqMessages.slice().reverse().find(m => m && m.role === 'user' && m.content)?.content || JSON.stringify(reqMessages))
        : String(query_data || ''));

  if (queryText.length > 10000) {
    return res.status(400).json({
      success: false,
      error: 'query_data 長度不得超過 10000 字元'
    });
  }

  const sessionId = `sess_${app_id}_${user_id}_${Date.now()}`;
  const startTime = Date.now();

  // 確保用戶路徑
  const localPath = getOrCreateUserPath(app_id, user_id);

  // 記錄 raw data
  rawDataLogger.log(app_id, user_id, queryText);

  // 記錄 session (初始狀態: queued)
  db.prepare(`
    INSERT INTO sessions (session_id, app_id, user_id, status, query_preview)
    VALUES (?, ?, ?, 'queued', ?)
  `).run(sessionId, app_id, user_id, queryText.substring(0, 200));

  try {
    // 取得 API Key（可能排隊）
    const apiKey = await keyPool.acquire(sessionId);

    // 更新 session 狀態
    db.prepare('UPDATE sessions SET status = ?, api_key_id = ? WHERE session_id = ?')
      .run('active', apiKey.id, sessionId);

    // 取得用戶歷史彙整作為背景
    const contextHistory = getUserContextHistory(app_id, user_id);

    // 調用 AI（確保 finally 中一定 release）
    let aiResponse;
    try {
      aiResponse = await callAI(apiKey, query_data, contextHistory, reqMessages);
    } finally {
      // 無論成功或失敗，都釋放 API Key 併發槽位
      keyPool.release(apiKey.id, sessionId);
    }

    const duration = Date.now() - startTime;

    // 更新 session 完成狀態
    db.prepare(`
      UPDATE sessions SET status = 'completed', response_preview = ?, completed_at = ?, duration_ms = ?
      WHERE session_id = ?
    `).run(aiResponse.substring(0, 200), new Date().toISOString(), duration, sessionId);

    // 更新用戶最後活躍時間
    db.prepare('UPDATE users SET last_active = ? WHERE app_id = ? AND user_id = ?')
      .run(new Date().toISOString(), app_id, user_id);

    res.json({
      success: true,
      session_id: sessionId,
      response: aiResponse,
      local_path: localPath,
      duration_ms: duration,
      context_used: contextHistory.length > 0,
    });

  } catch (err) {
    // 更新 session 失敗狀態
    db.prepare("UPDATE sessions SET status = 'failed', response_preview = ?, completed_at = ? WHERE session_id = ?")
      .run(err.message.substring(0, 200), new Date().toISOString(), sessionId);

    res.status(500).json({
      success: false,
      session_id: sessionId,
      error: err.message,
    });
  }
});

/**
 * GET /api/health — 健康檢查
 */
app.get('/api/health', (req, res) => {
  const poolStatus = keyPool.getStatus();
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    keyPool: poolStatus,
    version: '2.2.0',
  });
});

// ---- 管理 API ----

// API Keys 管理
app.get('/api/admin/keys', adminAuth, (req, res) => {
  const keys = db.prepare('SELECT id, label, base_url, model, is_active, total_calls, last_used, rate_limit, max_concurrent FROM api_keys ORDER BY id').all();
  // 附加當前併發狀態
  const poolStatus = keyPool.getStatus();
  for (const key of keys) {
    const poolKey = poolStatus.keys.find(k => k.id === key.id);
    if (poolKey) {
      key.current_concurrent = poolKey.concurrent;
      key.rate_used = poolKey.rateUsed;
    }
  }
  res.json({ success: true, data: keys });
});

app.post('/api/admin/keys', adminAuth, (req, res) => {
  const { key_value, label, base_url, model, rate_limit, max_concurrent } = req.body;
  if (!key_value) return res.status(400).json({ error: '缺少 key_value' });

  try {
    const result = db.prepare(
      'INSERT INTO api_keys (key_value, label, base_url, model, rate_limit, max_concurrent) VALUES (?, ?, ?, ?, ?, ?)'
    ).run(
      key_value,
      label || '',
      base_url || CONFIG.defaultAIBaseUrl,
      model || CONFIG.defaultAIModel,
      rate_limit || 10,
      max_concurrent || CONFIG.maxConcurrentPerKey
    );
    keyPool.reload();
    res.json({ success: true, id: result.lastInsertRowid });
  } catch (err) {
    if (err.message.includes('UNIQUE')) {
      return res.status(409).json({ error: 'API Key 已存在' });
    }
    throw err;
  }
});

app.put('/api/admin/keys/:id', adminAuth, (req, res) => {
  const { label, base_url, model, is_active, rate_limit, max_concurrent } = req.body;
  db.prepare(`
    UPDATE api_keys SET
      label = COALESCE(?, label),
      base_url = COALESCE(?, base_url),
      model = COALESCE(?, model),
      is_active = COALESCE(?, is_active),
      rate_limit = COALESCE(?, rate_limit),
      max_concurrent = COALESCE(?, max_concurrent)
    WHERE id = ?
  `).run(label, base_url, model, is_active, rate_limit, max_concurrent, req.params.id);
  keyPool.reload();
  res.json({ success: true });
});

app.delete('/api/admin/keys/:id', adminAuth, (req, res) => {
  db.prepare('DELETE FROM api_keys WHERE id = ?').run(req.params.id);
  keyPool.reload();
  res.json({ success: true });
});

// Apps 管理
app.get('/api/admin/apps', adminAuth, (req, res) => {
  const apps = db.prepare('SELECT * FROM apps ORDER BY created_at DESC').all();
  for (const a of apps) {
    a.user_count = db.prepare('SELECT COUNT(*) as c FROM users WHERE app_id = ?').get(a.app_id).c;
  }
  res.json({ success: true, data: apps });
});

// Users 管理
app.get('/api/admin/users', adminAuth, (req, res) => {
  const { app_id } = req.query;
  let users;
  if (app_id) {
    users = db.prepare('SELECT * FROM users WHERE app_id = ? ORDER BY last_active DESC').all(app_id);
  } else {
    users = db.prepare('SELECT * FROM users ORDER BY last_active DESC LIMIT 200').all();
  }
  res.json({ success: true, data: users });
});

// Sessions 管理
app.get('/api/admin/sessions', adminAuth, (req, res) => {
  const { app_id, user_id, status, limit } = req.query;
  let sql = 'SELECT * FROM sessions WHERE 1=1';
  const params = [];

  if (app_id) { sql += ' AND app_id = ?'; params.push(app_id); }
  if (user_id) { sql += ' AND user_id = ?'; params.push(user_id); }
  if (status) { sql += ' AND status = ?'; params.push(status); }

  sql += ' ORDER BY created_at DESC LIMIT ?';
  params.push(parseInt(limit) || 100);

  const sessions = db.prepare(sql).all(...params);
  res.json({ success: true, data: sessions });
});

// Summaries 管理
app.get('/api/admin/summaries', adminAuth, (req, res) => {
  const { app_id, user_id } = req.query;
  let sql = 'SELECT * FROM summaries WHERE 1=1';
  const params = [];

  if (app_id) { sql += ' AND app_id = ?'; params.push(app_id); }
  if (user_id) { sql += ' AND user_id = ?'; params.push(user_id); }

  sql += ' ORDER BY week_start DESC LIMIT 50';
  const summaries = db.prepare(sql).all(...params);
  res.json({ success: true, data: summaries });
});


// ---- 刪除端點 ----

// 刪除 App（含其所有 users + sessions + summaries + 檔案）
app.delete('/api/admin/apps/:app_id', adminAuth, (req, res) => {
  const { app_id } = req.params;
  const app = db.prepare('SELECT * FROM apps WHERE app_id = ?').get(app_id);
  if (!app) return res.status(404).json({ success: false, error: 'App 不存在' });

  const userCount = db.prepare('SELECT COUNT(*) as c FROM users WHERE app_id = ?').get(app_id).c;
  const sessionCount = db.prepare('SELECT COUNT(*) as c FROM sessions WHERE app_id = ?').get(app_id).c;
  const summaryCount = db.prepare('SELECT COUNT(*) as c FROM summaries WHERE app_id = ?').get(app_id).c;

  // 刪除關聯數據
  db.prepare('DELETE FROM summaries WHERE app_id = ?').run(app_id);
  db.prepare('DELETE FROM sessions WHERE app_id = ?').run(app_id);
  db.prepare('DELETE FROM users WHERE app_id = ?').run(app_id);
  db.prepare('DELETE FROM apps WHERE app_id = ?').run(app_id);

  // 刪除檔案系統數據
  const dataDir = path.join(__dirname, 'data', app_id);
  if (fs.existsSync(dataDir)) {
    fs.rmSync(dataDir, { recursive: true, force: true });
  }

  res.json({ success: true, deleted: { app_id, users: userCount, sessions: sessionCount, summaries: summaryCount } });
});

// 刪除 User（含其 sessions + summaries + 檔案）
app.delete('/api/admin/users/:app_id/:user_id', adminAuth, (req, res) => {
  const { app_id, user_id } = req.params;
  const user = db.prepare('SELECT * FROM users WHERE app_id = ? AND user_id = ?').get(app_id, user_id);
  if (!user) return res.status(404).json({ success: false, error: 'User 不存在' });

  const sessionCount = db.prepare('SELECT COUNT(*) as c FROM sessions WHERE app_id = ? AND user_id = ?').get(app_id, user_id).c;
  const summaryCount = db.prepare('SELECT COUNT(*) as c FROM summaries WHERE app_id = ? AND user_id = ?').get(app_id, user_id).c;

  db.prepare('DELETE FROM summaries WHERE app_id = ? AND user_id = ?').run(app_id, user_id);
  db.prepare('DELETE FROM sessions WHERE app_id = ? AND user_id = ?').run(app_id, user_id);
  db.prepare('DELETE FROM users WHERE app_id = ? AND user_id = ?').run(app_id, user_id);

  // 刪除檔案系統數據
  const dataDir = path.join(__dirname, 'data', app_id, user_id);
  if (fs.existsSync(dataDir)) {
    fs.rmSync(dataDir, { recursive: true, force: true });
  }

  res.json({ success: true, deleted: { app_id, user_id, sessions: sessionCount, summaries: summaryCount } });
});

// 刪除 Session
app.delete('/api/admin/sessions/:id', adminAuth, (req, res) => {
  const { id } = req.params;
  const session = db.prepare('SELECT * FROM sessions WHERE id = ?').get(id);
  if (!session) return res.status(404).json({ success: false, error: 'Session 不存在' });

  db.prepare('DELETE FROM sessions WHERE id = ?').run(id);
  res.json({ success: true, deleted: { id, session_id: session.session_id } });
});

// 刪除 Summary
app.delete('/api/admin/summaries/:id', adminAuth, (req, res) => {
  const { id } = req.params;
  const summary = db.prepare('SELECT * FROM summaries WHERE id = ?').get(id);
  if (!summary) return res.status(404).json({ success: false, error: 'Summary 不存在' });

  db.prepare('DELETE FROM summaries WHERE id = ?').run(id);
  res.json({ success: true, deleted: { id } });
});

// 批量清空 Sessions
app.delete('/api/admin/sessions', adminAuth, (req, res) => {
  const { app_id, before_date } = req.query;
  let sql = 'DELETE FROM sessions WHERE 1=1';
  const params = [];
  if (app_id) { sql += ' AND app_id = ?'; params.push(app_id); }
  if (before_date) { sql += ' AND created_at < ?'; params.push(before_date); }
  const result = db.prepare(sql).run(...params);
  res.json({ success: true, deleted: result.changes });
});

// 手動觸發彙整
app.post('/api/admin/trigger-summary', adminAuth, async (req, res) => {
  res.json({ success: true, message: '彙整已開始' });
  runDailySummary().catch(err => console.error('[Summary] Error:', err));
});

// 統計
app.get('/api/admin/stats', adminAuth, (req, res) => {
  const totalApps = db.prepare('SELECT COUNT(*) as c FROM apps').get().c;
  const totalUsers = db.prepare('SELECT COUNT(*) as c FROM users').get().c;
  const totalSessions = db.prepare('SELECT COUNT(*) as c FROM sessions').get().c;
  const todaySessions = db.prepare(
    "SELECT COUNT(*) as c FROM sessions WHERE created_at >= date('now')"
  ).get().c;
  const poolStatus = keyPool.getStatus();

  res.json({
    success: true,
    data: {
      apps: totalApps,
      users: totalUsers,
      totalSessions,
      todaySessions,
      keyPool: poolStatus,
    }
  });
});

// ---- 管理 Web UI ----
app.use('/admin', express.static(path.join(__dirname, 'public')));

// SPA fallback for admin UI
app.get('/admin/*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ============================================
// 定時任務：每日 23:55 彙整
// ============================================
function scheduleDailySummary() {
  const now = new Date();
  const [h, m] = CONFIG.summaryTime.split(':').map(Number);
  const target = new Date(now);
  target.setHours(h, m, 0, 0);
  if (target <= now) {
    target.setDate(target.getDate() + 1);
  }
  const delay = target - now;
  console.log(`[Scheduler] 下次彙整: ${target.toISOString()} (${Math.round(delay / 60000)} 分鐘後)`);

  setTimeout(() => {
    runDailySummary().catch(err => console.error('[Summary] Error:', err));
    // 每 24 小時重複
    setInterval(() => {
      runDailySummary().catch(err => console.error('[Summary] Error:', err));
    }, 24 * 60 * 60 * 1000);
  }, delay);
}

// 每 5 分鐘 flush raw data
setInterval(() => {
  rawDataLogger.flushAll();
}, 5 * 60 * 1000);

// 優雅關閉
// 全局錯誤處理 — 防止未捕獲的異常導致進程崩潰
process.on('unhandledRejection', (reason, promise) => {
  console.error('[Server] 未處理的 Promise 拒絕:', reason);
  // 不退出進程，只記錄錯誤
});

process.on('uncaughtException', (err) => {
  console.error('[Server] 未捕獲的異常:', err.message);
  console.error(err.stack);
  // 記錄後不退出，讓服務繼續運行
});

process.on('SIGTERM', () => {
  console.log('\n[Server] 收到 SIGTERM，正在關閉...');
  keyPool.destroy();
  rawDataLogger.flushAll();
  db.close();
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('\n[Server] 正在關閉...');
  keyPool.destroy();
  rawDataLogger.flushAll();
  db.close();
  process.exit(0);
});

// ============================================
// 啟動
// ============================================
app.listen(CONFIG.port, '127.0.0.1', () => {
  const poolStatus = keyPool.getStatus();
  console.log(`🤖 AI Gateway Server v2.2 已啟動: http://127.0.0.1:${CONFIG.port}`);
  console.log(`📊 管理介面: http://127.0.0.1:${CONFIG.port}/admin`);
  console.log(`🔑 API Key 池: ${poolStatus.totalKeys} 個 (總併發容量: ${poolStatus.maxTotalConcurrent})`);
  console.log(`📁 數據目錄: ${CONFIG.dataDir}`);
  console.log(`⏰ 彙整時間: ${CONFIG.summaryTime}`);
  console.log(`🔐 Admin Token: ${CONFIG.adminToken.substring(0, 8)}...`);
  console.log(`🛡️ CORS Origins: ${CONFIG.corsOrigins || '(同源限制)'}`);
  console.log(`📦 Session 保留: ${CONFIG.sessionArchiveDays} 天`);
  console.log(`⚡ 每 key 最大併發: ${CONFIG.maxConcurrentPerKey} | 僵死超時: ${CONFIG.keyStaleTimeout / 1000}s`);
  scheduleDailySummary();
});