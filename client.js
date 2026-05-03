/**
 * AI Response Server - 客户端 (NVIDIA AI API)
 */

const net = require('net');

// ============ TCP Socket 客户端 ============

class AIRSClient {
  constructor(options = {}) {
    this.host = options.host || 'localhost';
    this.port = options.port || 3001;
    this.appId = options.appId;
    this.userId = options.userId;
    this.socket = null;
    this.sessionId = null;
    this.connected = false;
    this.history = [];
    
    this.onConnect = options.onConnect || (() => {});
    this.onResponse = options.onResponse || (() => {});
    this.onError = options.onError || (() => {});
    this.onQueued = options.onQueued || (() => {});
    this.onHistory = options.onHistory || (() => {});
    this.onClose = options.onClose || (() => {});
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = new net.Socket();

      this.socket.connect(this.port, this.host, () => {
        this.send({
          type: 'connect',
          appId: this.appId,
          userId: this.userId
        });
      });

      this.socket.on('data', (data) => {
        const messages = data.toString().split('\n').filter(m => m.trim());
        for (const msg of messages) {
          try {
            this.handleMessage(JSON.parse(msg));
          } catch (e) {
            console.error('解析失败:', e);
          }
        }
      });

      this.socket.on('close', () => {
        this.connected = false;
        this.onClose();
      });

      this.socket.on('error', (e) => {
        this.onError(e);
        if (!this.connected) reject(e);
      });

      setTimeout(() => {
        if (!this.connected) reject(new Error('连接超时'));
      }, 10000);
    });
  }

  handleMessage(msg) {
    switch (msg.type) {
      case 'connected':
        this.sessionId = msg.sessionId;
        this.connected = true;
        this.onConnect(msg);
        break;
      
      case 'history':
        this.history = msg.data || [];
        this.onHistory(this.history);
        break;
      
      case 'response':
        this.onResponse(msg.data, msg);
        break;
      
      case 'error':
        this.onError(new Error(msg.message));
        break;
      
      case 'queued':
        this.onQueued(msg.position);
        break;
      
      case 'pong':
        break;
    }
  }

  send(data) {
    if (this.socket && !this.socket.destroyed) {
      this.socket.write(JSON.stringify(data) + '\n');
    }
  }

  /**
   * 发送问题并等待回复
   * @param {string} question - 用户问题
   * @returns {Promise<object>} AI 回复
   */
  ask(question) {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.onResponse = () => {};
        reject(new Error('请求超时 (60s)'));
      }, 60000);

      const originalResponse = this.onResponse;
      this.onResponse = (data, meta) => {
        clearTimeout(timeout);
        this.onResponse = originalResponse;
        resolve({ data, ...meta });
      };

      this.send({
        type: 'request',
        appId: this.appId,
        userId: this.userId,
        data: question
      });
    });
  }

  /**
   * 获取历史记录
   * @param {number} weekOffset - 周偏移量 (0=本周)
   */
  requestHistory(weekOffset = 0) {
    this.send({ type: 'history-request', weekOffset });
  }

  ping() {
    this.send({ type: 'ping' });
  }

  close() {
    if (this.socket) {
      this.socket.destroy();
    }
  }
}

// ============ WebSocket 客户端 (浏览器) ============

class AIRSWebSocketClient {
  constructor(options = {}) {
    this.url = options.url || 'ws://localhost:3000/ws';
    this.appId = options.appId;
    this.userId = options.userId;
    this.ws = null;
    this.sessionId = null;
    this.connected = false;
    this.history = [];
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = options.maxReconnectAttempts || 5;
    
    this.onConnect = options.onConnect || (() => {});
    this.onResponse = options.onResponse || (() => {});
    this.onError = options.onError || (() => {});
    this.onHistory = options.onHistory || (() => {});
    this.onClose = options.onClose || (() => {});
    this.onReconnecting = options.onReconnecting || (() => {});
  }

  connect() {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
          this.send({
            type: 'connect',
            appId: this.appId,
            userId: this.userId
          });
          this.connected = true;
          this.reconnectAttempts = 0;
        };

        this.ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            this.handleMessage(msg);
          } catch (e) {
            console.error('解析失败:', e);
          }
        };

        this.ws.onerror = (e) => {
          this.onError(e);
        };

        this.ws.onclose = () => {
          this.connected = false;
          this.onClose();
          this.attemptReconnect();
        };

        setTimeout(() => {
          if (!this.connected) reject(new Error('连接超时'));
        }, 10000);
      } catch (e) {
        reject(e);
      }
    });
  }

  handleMessage(msg) {
    switch (msg.type) {
      case 'connected':
        this.sessionId = msg.sessionId;
        this.onConnect(msg);
        break;
      
      case 'history':
        this.history = msg.data || [];
        this.onHistory(this.history);
        break;
      
      case 'response':
        this.onResponse(msg.data);
        break;
      
      case 'error':
        this.onError(new Error(msg.message));
        break;
    }
  }

  send(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    }
  }

  ask(question) {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.onResponse = () => {};
        reject(new Error('请求超时'));
      }, 60000);

      const originalResponse = this.onResponse;
      this.onResponse = (data) => {
        clearTimeout(timeout);
        this.onResponse = originalResponse;
        resolve(data);
      };

      this.send({
        type: 'request',
        appId: this.appId,
        userId: this.userId,
        data: question
      });
    });
  }

  attemptReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      return;
    }
    
    this.reconnectAttempts++;
    this.onReconnecting(this.reconnectAttempts);
    
    setTimeout(() => {
      this.connect().catch(() => {});
    }, Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000));
  }

  close() {
    this.maxReconnectAttempts = 0; // 防止自动重连
    if (this.ws) {
      this.ws.close();
    }
  }
}

// ============ HTTP 客户端 ============

class AIRSHTTPClient {
  constructor(baseUrl = 'http://localhost:3000') {
    this.baseUrl = baseUrl;
  }

  /**
   * 发送 AI 对话请求
   */
  async chat(appId, userId, message, options = {}) {
    const response = await fetch(`${this.baseUrl}/api/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        appId,
        userId,
        message,
        model: options.model,
        history: options.history
      })
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || '请求失败');
    }

    return await response.json();
  }

  /**
   * 获取系统状态
   */
  async getStatus() {
    const response = await fetch(`${this.baseUrl}/api/status`);
    return await response.json();
  }

  /**
   * 获取支持的模型
   */
  async getModels() {
    const response = await fetch(`${this.baseUrl}/api/models`);
    return await response.json();
  }
}

// ============ 导出 ============

module.exports = { 
  AIRSClient, 
  AIRSWebSocketClient, 
  AIRSHTTPClient 
};

// ============ 使用示例 ============

async function tcpExample() {
  console.log('=== TCP 客户端示例 (NVIDIA AI) ===\n');
  
  const client = new AIRSClient({
    host: 'localhost',
    port: 3001,
    appId: 'demo',
    userId: 'user1',
    
    onConnect: (info) => {
      console.log('[连接] 成功!');
      console.log('[服务器]', info.server);
      console.log('[Session]', info.sessionId);
    },
    
    onHistory: (history) => {
      console.log(`\n[历史] 已加载 ${history.length} 条记录`);
    },
    
    onResponse: (data, meta) => {
      const content = data.choices?.[0]?.message?.content;
      console.log('\n[AI 回复]', content);
      if (meta.duration) {
        console.log(`[耗时] ${meta.duration}ms`);
      }
      if (meta.model) {
        console.log(`[模型] ${meta.model}`);
      }
    },
    
    onQueued: (position) => {
      console.log(`\n[排队] 位置: ${position}`);
    },
    
    onError: (e) => {
      console.error('\n[错误]', e.message);
    },
    
    onClose: () => {
      console.log('\n[断开] 连接已关闭');
    }
  });

  try {
    await client.connect();
    
    // 发送问题
    const questions = [
      '你好，请介绍一下 Llama 3.1 模型',
      '它和 GPT-4 相比有什么优势?'
    ];
    
    for (const q of questions) {
      console.log(`\n[提问] ${q}`);
      try {
        const result = await client.ask(q);
        console.log('[回复]', result.data.choices?.[0]?.message?.content);
      } catch (e) {
        console.error('[失败]', e.message);
      }
    }
    
    // 保持连接发送心跳
    setInterval(() => client.ping(), 30000);
    
  } catch (e) {
    console.error('连接失败:', e.message);
    console.log('请确保服务器已启动: npm start');
  }
}

// 运行
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args[0] === 'http') {
    // HTTP 客户端示例
    const http = new AIRSHTTPClient();
    
    (async () => {
      console.log('=== HTTP 客户端示例 ===\n');
      
      // 获取状态
      const status = await http.getStatus();
      console.log('系统状态:', status);
      
      // 获取模型列表
      const models = await http.getModels();
      console.log('\n可用模型:', models);
      
      // 发送问题
      const result = await http.chat('demo', 'user1', '你好');
      console.log('\nAI 回复:', result.response);
    })();
  } else {
    // TCP 客户端
    tcpExample();
  }
}
