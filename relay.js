/* gf-relay — 3D 枪战 WebSocket 中继
 *
 * 作用：绕开不支持 WebSocket 的平台网关。
 * 链路：浏览器 --WS--> 中继 --HTTP(LP 通道)--> 游戏服
 *  - 下行：中继保持一条 /lp/<cid>/stream 流式连接，NDJSON 逐行转发给浏览器
 *  - 上行：浏览器 WS 消息立即批量 POST /lp/send（最多 4 条并发，无浏览器连接数限制）
 *  - 410（会话被回收）或浏览器断开 → 中继调用 /lp/close 清理
 *
 * 环境变量：
 *  - GAME_ORIGIN  游戏服地址（默认线上地址）
 *  - PORT         监听端口（托管平台自动注入）
 *
 * 部署：见同目录 README.md
 */
'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');

const GAME = (process.env.GAME_ORIGIN || 'https://3dgunfight.app.workbuddy.host').replace(/\/+$/, '');
const PORT = Number(process.env.PORT) || 8787;
const MAX_UP_INFLIGHT = 4;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('gf-relay ok -> ' + GAME);
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  let cid = null;
  let opened = false;
  let closed = false;
  const upQ = [];          // 待上行消息（原始字符串）
  let upInflight = 0;
  let downAbort = null;

  const gfetch = (path, opts) => fetch(GAME + path, opts);

  function safeSend(text) {
    if (!closed && ws.readyState === 1) {
      try { ws.send(text); } catch (e) {}
    }
  }

  function die() {
    if (closed) return;
    closed = true;
    if (downAbort) { try { downAbort.abort(); } catch (e) {} downAbort = null; }
    if (cid) {
      const c = cid;
      cid = null;
      gfetch('/lp/close/' + c, { method: 'POST', body: '' }).catch(() => {});
    }
    try { ws.close(); } catch (e) {}
  }

  async function open(firstMsgText) {
    try {
      const r = await gfetch('/lp/open', { method: 'POST', body: firstMsgText });
      if (!r.ok) return die();
      const d = await r.json();
      if (!d || !d.cid) return die();
      cid = d.cid;
      opened = true;
      for (const line of d.out || []) safeSend(line);
      startDown();
      flush();
    } catch (e) { die(); }
  }

  function flush() {
    if (closed || !opened) return;
    while (upInflight < MAX_UP_INFLIGHT && upQ.length) {
      const batch = upQ.splice(0, upQ.length);
      upInflight++;
      gfetch('/lp/send/' + cid, { method: 'POST', body: JSON.stringify(batch) })
        .then(async (r) => {
          upInflight--;
          if (closed) return;
          if (r.status === 410) return die();   // 会话已被游戏服回收
          if (!r.ok) throw new Error('send ' + r.status);
          const d = await r.json();
          for (const line of d.out || []) safeSend(line);
          flush();
        })
        .catch(() => {
          upInflight--;
          // 网关抖动：短暂退避后原样重发（state 有序号，服务端拒收旧包，安全）
          if (closed) return;
          setTimeout(() => {
            if (closed) return;
            upQ.unshift(...batch);
            flush();
          }, 250);
        });
    }
  }

  function startDown() {
    if (closed || !cid) return;
    const ctl = new AbortController();
    downAbort = ctl;
    gfetch('/lp/' + cid + '/stream', { signal: ctl.signal }).then((r) => {
      if (closed) return;
      if (!r.ok || !r.body) return;   // 流不可用不致命：/lp/send 响应里仍带回下行
      const reader = r.body.getReader();
      const dec = new TextDecoder();
      let buf = '';
      const read = () => reader.read().then(({ done, value }) => {
        if (closed) return;
        if (done) { setTimeout(startDown, 300); return; }   // 断流自动重连
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, i).trim();
          buf = buf.slice(i + 1);
          if (!line) continue;
          safeSend(line);
        }
        read();
      }).catch(() => { if (!closed) setTimeout(startDown, 300); });
      read();
    }).catch(() => {});
  }

  ws.on('message', (data) => {
    if (closed) return;
    const text = data.toString();
    if (!opened) { open(text); return; }   // 首条消息视为 join（原文转发）
    let obj;
    try { obj = JSON.parse(text); } catch (e) { return; }
    if (upQ.length < 128) upQ.push(obj);   // 必须是对象：/lp/send 期望对象数组
    flush();
  });

  ws.on('close', die);
  ws.on('error', die);

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
});

// 心跳：清掉死掉的浏览器连接
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { try { ws.terminate(); } catch (e) {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 15000);

server.listen(PORT, () => {
  console.log('[gf-relay] listening on :' + PORT + '  ->  ' + GAME);
});
