/**
 * AI Gateway - 客戶端使用示例
 */

const { AIRSClient, AIRSWebSocketClient, AIRSHTTPClient } = require('./client');

// ============ TCP 客戶端示例 ============

async function tcpExample() {
  console.log('=== TCP 客戶端示例 ===\n');

  const client = new AIRSClient({
    host: 'localhost',
    port: 3005,
    appId: 'demo',
    userId: 'user1',

    onConnect: (info) => {
      console.log('[連接] 成功!');
      console.log('[Session]', info.sessionId);
    },

    onHistory: (history) => {
      console.log(`\n[歷史] 已加載 ${history.length} 條記錄`);
      history.forEach((h, i) => {
        console.log(`  ${i + 1}. ${h.role}: ${h.content.substring(0, 50)}...`);
      });
    },

    onResponse: (data, meta) => {
      const content = data.choices?.[0]?.message?.content;
      console.log('\n[AI 回覆]', content);
      if (meta.duration) {
        console.log(`[耗時] ${meta.duration}ms`);
      }
    },

    onQueued: (position) => {
      console.log(`\n[排隊] 位置: ${position}`);
    },

    onError: (e) => {
      console.error('\n[錯誤]', e.message);
    },

    onClose: () => {
      console.log('\n[斷開] 連接已關閉');
    }
  });

  try {
    await client.connect();
    console.log('\n已連接到服務器\n');

    // 請求歷史
    client.requestHistory();

    // 發送問題
    const questions = [
      '你好，請介紹一下你自己',
      '你有什麼能力？'
    ];

    for (const q of questions) {
      console.log(`\n[提問] ${q}`);
      try {
        const result = await client.ask(q);
        console.log('[回覆]', result.data?.choices?.[0]?.message?.content || '無回覆');
      } catch (e) {
        console.error('[失敗]', e.message);
      }
    }

    // 保持連接
    setTimeout(() => client.close(), 1000);

  } catch (e) {
    console.error('連接失敗:', e.message);
    console.log('請確保服務器已啟動: npm start');
  }
}

// ============ HTTP 客戶端示例 ============

async function httpExample() {
  console.log('=== HTTP 客戶端示例 ===\n');

  const http = new AIRSHTTPClient('http://localhost:3005');

  try {
    // 獲取系統狀態
    console.log('獲取系統狀態...');
    const status = await fetch('http://localhost:3005/api/health').then(r => r.json());
    console.log('系統狀態:', JSON.stringify(status, null, 2));

    // 發送查詢
    console.log('\n發送查詢...');
    const response = await fetch('http://localhost:3005/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        app_id: 'demo',
        user_id: 'user1',
        query_data: '你好，請介紹一下你自己'
      })
    });

    const result = await response.json();
    console.log('\n結果:', JSON.stringify(result, null, 2));

  } catch (e) {
    console.error('請求失敗:', e.message);
  }
}

// ============ 移動應用接入示例 ============

function mobileExample() {
  console.log('=== 移動應用接入示例 ===\n');

  console.log(`
移動應用可以使用 HTTP API 接入：

// 1. 發送問題
fetch('https://your-server.com/api/query', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    app_id: 'your_app_id',
    user_id: 'user_device_id',
    query_data: '用戶問題'
  })
})
.then(res => res.json())
.then(data => {
  console.log('AI 回覆:', data.response);
});

// 2. 攜帶歷史對話（可選）
fetch('https://your-server.com/api/query', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    app_id: 'your_app_id',
    user_id: 'user_device_id',
    query_data: '跟進問題',
    messages: [
      { role: 'user', content: '之前問的問題' },
      { role: 'assistant', content: '之前的回覆' },
      { role: 'user', content: '跟進問題' }
    ]
  })
});
`);
}

// 運行示例
if (require.main === module) {
  const args = process.argv.slice(2);
  const mode = args[0] || 'tcp';

  console.log('AI Gateway 客戶端示例\n');
  console.log('='.repeat(50));

  if (mode === 'http') {
    httpExample();
  } else if (mode === 'mobile') {
    mobileExample();
  } else {
    tcpExample();
  }
}

module.exports = { tcpExample, httpExample, mobileExample };
