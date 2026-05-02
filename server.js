/**
 * AI Gateway Server — 多應用 AI API 閘道器
 * 
 * 核心功能：
 * 1. 三層索引數據庫 (app_id → user_id → local_path)
 * 2. TCP/IP Session 管理 + API Key 池化
 * 3. 每日 Raw Data 記錄 + 23:55 AI 彙整
 * 4. 管理 Web UI
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
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
  summaryTime: process.env.SUMMARY_TIME || '23:55',     // 每日彙整觸發時間
  summaryModel: process.env.SUMMARY_MODEL || 'meta/llama-3.3-70b-instruct', // 彙整用更強模型
  maxQueueSize: parseInt(process.env.MAX_QUEUE_SIZE || '100'),
  sessionTimeout: parseInt(process.env.SESSION_TIMEOUT || '300000'), // 5 分鐘
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
    rate_limit INTEGER DEFAULT 10  -- 每分鐘最大請求數
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
// API Key 池化管理器
// ============================================
class APIKeyPool {
  constructor() {
    this.keys = [];
    this.locked = new Map();   // apiKeyId → sessionId
    this.queue = [];            // 排隊的 session
    this.rateCounters = new Map(); // apiKeyId → { count, resetAt }
    this._loadKeys();
  }

  _loadKeys() {
    const rows = db.prepare('SELECT * FROM api_keys WHERE is_active = 1').all();
    this.keys = rows;
    console.log(`[APIKeyPool] 載入 ${rows.length} 個活躍 API Key`);
  }

  reload() {
    this._loadKeys();
  }

  async acquire(sessionId) {
    // 找一個可用的 key（未鎖定 + 未超過速率限制）
    for (const key of this.keys) {
      if (this.locked.has(key.id)) continue;
      if (!this._checkRateLimit(key.id)) continue;

      // 鎖定此 key 給此 session
      this.locked.set(key.id, sessionId);
      this._incrementRate(key.id);

      // 更新最後使用時間
      db.prepare('UPDATE api_keys SET last_used = ?, total_calls = total_calls + 1 WHERE id = ?')
        .run(new Date().toISOString(), key.id);

      return key;
    }

    // 沒有可用 key → 加入佇列
    if (this.queue.length >= CONFIG.maxQueueSize) {
      throw new Error('Queue full — too many concurrent requests');
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const idx = this.queue.findIndex(q => q.sessionId === sessionId);
        if (idx !== -1) this.queue.splice(idx, 1);
        reject(new Error('Session queued timeout'));
      }, CONFIG.sessionTimeout);

      this.queue.push({ sessionId, resolve, reject, timeout });
      console.log(`[APIKeyPool] Session ${sessionId} 排隊中 (佇列長度: ${this.queue.length})`);
    });
  }

  release(keyId) {
    this.locked.delete(keyId);

    // 如果佇列中有等待的 session，分配 key
    if (this.queue.length > 0) {
      const key = this.keys.find(k => k.id === keyId && !this.locked.has(keyId));
      if (key && this.queue.length > 0) {
        const next = this.queue.shift();
        clearTimeout(next.timeout);
        this.locked.set(keyId, next.sessionId);
        this._incrementRate(keyId);
        db.prepare('UPDATE api_keys SET last_used = ?, total_calls = total_calls + 1 WHERE id = ?')
          .run(new Date().toISOString(), keyId);
        next.resolve(key);
      }
    }
  }

  _checkRateLimit(keyId) {
    const counter = this.rateCounters.get(keyId);
    if (!counter) return true;
    if (Date.now() > counter.resetAt) {
      this.rateCounters.delete(keyId);
      return true;
    }
    const key = this.keys.find(k => k.id === keyId);
    return counter.count < (key?.rate_limit || 10);
  }

  _incrementRate(keyId) {
    let counter = this.rateCounters.get(keyId);
    if (!counter || Date.now() > counter.resetAt) {
      counter = { count: 0, resetAt: Date.now() + 60000 };
    }
    counter.count++;
    this.rateCounters.set(keyId, counter);
  }

  getStatus() {
    return {
      totalKeys: this.keys.length,
      available: this.keys.filter(k => !this.locked.has(k.id)).length,
      locked: this.locked.size,
      queueLength: this.queue.length,
    };
  }
}

const keyPool = new APIKeyPool();

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
    const key = `${appId}_${userId}`;
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
    const [appId, userId] = key.split('_');
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
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`AI API error ${response.status}: ${errText}`);
  }

  const data = await response.json();
  return data.choices[0].message.content;
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

      // 使用彙整專用 API Key
      let summaryKey = keyPool.keys.find(k => k.id === Math.min(...keyPool.keys.map(kk => kk.id)));
      if (!summaryKey && keyPool.keys.length > 0) summaryKey = keyPool.keys[0];
      
      if (!summaryKey) {
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
          'Authorization': `Bearer ${summaryKey.key_value}`,
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
    ORDER BY week_start DESC LIMIT 4
  `).all(appId, userId);

  if (summaries.length === 0) return '';
  return summaries.map((s, i) => `=== 第 ${summaries.length - i} 週彙整 ===\n${s.summary_text}`).join('\n\n');
}

// ============================================
// Express 應用
// ============================================
const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// 根路徑重定向到管理介面
app.get('/', (req, res) => {
  const prefix = (req.headers['x-forwarded-prefix'] || '').replace(/\/$/, '');
  res.redirect(prefix + '/admin/');
});

// ---- 核心 API ----

/**
 * POST /api/query — 主要 AI 查詢端點
 * Body: { app_id, user_id, query_data, options? }
 * Response: { success, session_id, response, local_path }
 */
app.post('/api/query', async (req, res) => {
  const { app_id, user_id, query_data, options, messages: reqMessages } = req.body;

  if (!app_id || !user_id || (!query_data && (!reqMessages || reqMessages.length === 0))) {
    return res.status(400).json({
      success: false,
      error: '缺少必要欄位: app_id, user_id, query_data 或 messages'
    });
  }

  const sessionId = `sess_${app_id}_${user_id}_${Date.now()}`;
  const startTime = Date.now();

  // 確保用戶路徑
  const localPath = getOrCreateUserPath(app_id, user_id);

  // 記錄 raw data
  rawDataLogger.log(app_id, user_id, query_data);

  // 記錄 session (初始狀態: queued)
  db.prepare(`
    INSERT INTO sessions (session_id, app_id, user_id, status, query_preview)
    VALUES (?, ?, ?, 'queued', ?)
  `).run(sessionId, app_id, user_id, query_data.substring(0, 200));

  try {
    // 取得 API Key（可能排隊）
    const apiKey = await keyPool.acquire(sessionId);

    // 更新 session 狀態
    db.prepare('UPDATE sessions SET status = ?, api_key_id = ? WHERE session_id = ?')
      .run('active', apiKey.id, sessionId);

    // 取得用戶歷史彙整作為背景
    const contextHistory = getUserContextHistory(app_id, user_id);

    // 調用 AI
    const aiResponse = await callAI(apiKey, query_data, contextHistory, reqMessages);

    // 釋放 API Key
    keyPool.release(apiKey.id);

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
      .run(err.message, new Date().toISOString(), sessionId);

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
    version: '1.0.0',
  });
});

// ---- 管理 API ----

// API Keys 管理
app.get('/api/admin/keys', (req, res) => {
  const keys = db.prepare('SELECT id, label, base_url, model, is_active, total_calls, last_used, rate_limit FROM api_keys ORDER BY id').all();
  res.json({ success: true, data: keys });
});

app.post('/api/admin/keys', (req, res) => {
  const { key_value, label, base_url, model, rate_limit } = req.body;
  if (!key_value) return res.status(400).json({ error: '缺少 key_value' });

  try {
    const result = db.prepare(
      'INSERT INTO api_keys (key_value, label, base_url, model, rate_limit) VALUES (?, ?, ?, ?, ?)'
    ).run(key_value, label || '', base_url || CONFIG.defaultAIBaseUrl, model || CONFIG.defaultAIModel, rate_limit || 10);
    
    keyPool.reload();
    res.json({ success: true, id: result.lastInsertRowid });
  } catch (err) {
    if (err.message.includes('UNIQUE')) {
      return res.status(409).json({ error: 'API Key 已存在' });
    }
    throw err;
  }
});

app.put('/api/admin/keys/:id', (req, res) => {
  const { label, base_url, model, is_active, rate_limit } = req.body;
  db.prepare(`
    UPDATE api_keys SET label = COALESCE(?, label), base_url = COALESCE(?, base_url),
    model = COALESCE(?, model), is_active = COALESCE(?, is_active),
    rate_limit = COALESCE(?, rate_limit) WHERE id = ?
  `).run(label, base_url, model, is_active, rate_limit, req.params.id);
  keyPool.reload();
  res.json({ success: true });
});

app.delete('/api/admin/keys/:id', (req, res) => {
  db.prepare('DELETE FROM api_keys WHERE id = ?').run(req.params.id);
  keyPool.reload();
  res.json({ success: true });
});

// Apps 管理
app.get('/api/admin/apps', (req, res) => {
  const apps = db.prepare('SELECT * FROM apps ORDER BY created_at DESC').all();
  for (const a of apps) {
    a.user_count = db.prepare('SELECT COUNT(*) as c FROM users WHERE app_id = ?').get(a.app_id).c;
  }
  res.json({ success: true, data: apps });
});

// Users 管理
app.get('/api/admin/users', (req, res) => {
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
app.get('/api/admin/sessions', (req, res) => {
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
app.get('/api/admin/summaries', (req, res) => {
  const { app_id, user_id } = req.query;
  let sql = 'SELECT * FROM summaries WHERE 1=1';
  const params = [];
  if (app_id) { sql += ' AND app_id = ?'; params.push(app_id); }
  if (user_id) { sql += ' AND user_id = ?'; params.push(user_id); }
  sql += ' ORDER BY week_start DESC LIMIT 50';
  
  const summaries = db.prepare(sql).all(...params);
  res.json({ success: true, data: summaries });
});

// 手動觸發彙整
app.post('/api/admin/trigger-summary', async (req, res) => {
  res.json({ success: true, message: '彙整已開始' });
  runDailySummary().catch(err => console.error('[Summary] Error:', err));
});

// 統計
app.get('/api/admin/stats', (req, res) => {
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
process.on('SIGINT', () => {
  console.log('\n[Server] 正在關閉...');
  rawDataLogger.flushAll();
  db.close();
  process.exit(0);
});

// ============================================
// 啟動
// ============================================
app.listen(CONFIG.port, '127.0.0.1', () => {
  console.log(`🤖 AI Gateway Server 已啟動: http://127.0.0.1:${CONFIG.port}`);
  console.log(`📊 管理介面: http://127.0.0.1:${CONFIG.port}/admin`);
  console.log(`🔑 API Key 池: ${keyPool.getStatus().totalKeys} 個`);
  console.log(`📁 數據目錄: ${CONFIG.dataDir}`);
  console.log(`⏰ 彙整時間: ${CONFIG.summaryTime}`);

  scheduleDailySummary();
});
