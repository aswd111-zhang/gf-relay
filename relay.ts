// gf-relay for Deno Deploy
// 浏览器 --WebSocket--> 本中继 --HTTP流--> 游戏服
// 部署后地址形如 https://gf-relay-xxxx.deno.dev ，游戏内填该域名即可
const GAME_ORIGIN = Deno.env.get("GAME_ORIGIN") || "https://3dgunfight.app.workbuddy.host";
const UP_CONCURRENCY = 4;   // 上行并发
const BATCH = 16;           // 每个 POST 最多打包的消息数

Deno.serve((req) => {
  const url = new URL(req.url);

  if (url.pathname !== "/ws") {
    return new Response("gf-relay ok -> " + GAME_ORIGIN, {
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  // ---- WebSocket 升级 ----
  const { socket: ws, response } = Deno.upgradeWebSocket(req);
  let cid = null;
  let closed = false;
  let started = false;
  const upQ = [];
  const ac = new AbortController();

  let inflight = 0;
  const pump = () => {
    while (!closed && cid && upQ.length && inflight < UP_CONCURRENCY) {
      const batch = upQ.splice(0, BATCH);
      inflight++;
      fetch(`${GAME_ORIGIN}/lp/send/${cid}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(batch),
        signal: ac.signal,
      })
        .catch(() => {})
        .finally(() => { inflight--; pump(); });
    }
  };

  const startStream = async () => {
    try {
      const sr = await fetch(`${GAME_ORIGIN}/lp/${cid}/stream`, { signal: ac.signal });
      if (!sr.body) return;
      const reader = sr.body.getReader();
      const dec = new TextDecoder();
      let buf = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, i).trim();
          buf = buf.slice(i + 1);
          if (!line) continue;
          try { if (ws.readyState === 1) ws.send(line); } catch { return; }
        }
      }
    } catch { /* 中断即结束 */ }
  };

  ws.onmessage = (ev) => {
    if (closed) return;
    const text = String(ev.data);
    if (!started) {
      started = true;
      // 首条消息 = join，向游戏服开 LP 会话
      (async () => {
        try {
          const r = await fetch(GAME_ORIGIN + "/lp/open", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: text,
          });
          const j = await r.json();
          cid = j.cid;
          if (!cid) { try { ws.close(1011, "no cid"); } catch {} return; }
          startStream();
          pump();
        } catch {
          try { ws.close(1011, "open failed"); } catch {}
        }
      })();
      return;
    }
    // 后续消息：解析成对象进队列（游戏服要求数组）
    try {
      const obj = JSON.parse(text);
      if (upQ.length < 256) { upQ.push(obj); pump(); }
    } catch { /* 忽略坏包 */ }
  };

  ws.onclose = () => {
    if (closed) return;
    closed = true;
    try { ac.abort(); } catch {}
    if (cid) {
      fetch(`${GAME_ORIGIN}/lp/close/${cid}`, { method: "POST" }).catch(() => {});
    }
  };
  ws.onerror = () => {
    closed = true;
    try { ac.abort(); } catch {}
  };

  return response;
});
