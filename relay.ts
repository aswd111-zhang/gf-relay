// gf-relay for Deno Deploy
// 浏览器 --WebSocket--> 本中继 --HTTP(LP 通道)--> 游戏服
//  - 下行：/lp/open 与 /lp/send 响应里的 out[] + /lp/<cid>/stream 流式 NDJSON
//  - 上行：WS 消息批量 POST /lp/send（最多 4 并发）
const GAME = (Deno.env.get("GAME_ORIGIN") || "https://3dgunfight.app.workbuddy.host").replace(/\/+$/, "");
const MAX_UP_INFLIGHT = 4;

Deno.serve((req) => {
  const url = new URL(req.url);

  if (url.pathname !== "/ws") {
    return new Response("gf-relay ok -> " + GAME, {
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  const { socket: ws, response } = Deno.upgradeWebSocket(req);
  let cid = null;
  let opened = false;
  let closed = false;
  let upInflight = 0;
  const upQ = [];
  let downAbort = null;

  const gfetch = (path, opts) => fetch(GAME + path, opts);

  const safeSend = (text) => {
    if (!closed && ws.readyState === 1) {
      try { ws.send(text); } catch { /* ignore */ }
    }
  };

  const die = () => {
    if (closed) return;
    closed = true;
    if (downAbort) { try { downAbort.abort(); } catch { /* ignore */ } downAbort = null; }
    if (cid) {
      const c = cid;
      cid = null;
      gfetch("/lp/close/" + c, { method: "POST" }).catch(() => {});
    }
    try { ws.close(); } catch { /* ignore */ }
  };

  const startDown = () => {
    if (closed || !cid) return;
    const ctl = new AbortController();
    downAbort = ctl;
    gfetch(`/lp/${cid}/stream`, { signal: ctl.signal }).then((r) => {
      if (closed) return;
      if (!r.ok || !r.body) return;   // 流不可用不致命：send 响应里仍带回下行
      const reader = r.body.getReader();
      const dec = new TextDecoder();
      let buf = "";
      const read = () => reader.read().then(({ done, value }) => {
        if (closed) return;
        if (done) { setTimeout(startDown, 300); return; }   // 断流自动重连
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, i).trim();
          buf = buf.slice(i + 1);
          if (!line) continue;
          safeSend(line);
        }
        read();
      }).catch(() => { if (!closed) setTimeout(startDown, 300); });
      read();
    }).catch(() => {});
  };

  const flush = () => {
    if (closed || !opened) return;
    while (upInflight < MAX_UP_INFLIGHT && upQ.length) {
      const batch = upQ.splice(0, upQ.length);
      upInflight++;
      gfetch("/lp/send/" + cid, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(batch),
      }).then(async (r) => {
        upInflight--;
        if (closed) return;
        if (r.status === 410) return die();   // 会话已被游戏服回收
        if (!r.ok) throw new Error("send " + r.status);
        const d = await r.json();
        for (const line of d.out || []) safeSend(line);
        flush();
      }).catch(() => {
        upInflight--;
        if (closed) return;
        setTimeout(() => {
          if (closed) return;
          upQ.unshift(...batch);
          flush();
        }, 250);
      });
    }
  };

  const open = async (firstMsgText) => {
    try {
      const r = await gfetch("/lp/open", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: firstMsgText,
      });
      if (!r.ok) return die();
      const d = await r.json();
      if (!d || !d.cid) return die();
      cid = d.cid;
      opened = true;
      for (const line of d.out || []) safeSend(line);
      startDown();
      flush();
    } catch { die(); }
  };

  ws.onmessage = (ev) => {
    if (closed) return;
    const text = String(ev.data);
    if (!opened) { open(text); return; }   // 首条消息视为 join（原文转发）
    let obj;
    try { obj = JSON.parse(text); } catch { return; }
    if (upQ.length < 128) upQ.push(obj);   // 必须是对象：/lp/send 期望对象数组
    flush();
  };
  ws.onclose = () => die();
  ws.onerror = () => die();

  return response;
});
