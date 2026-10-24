# gf-relay 部署指南

一个几十行的小中继：浏览器用真正的 WebSocket 连它，它在服务器侧用 HTTP 通道转发到游戏服，绕开平台网关的 WebSocket 拦截。

## 部署到 Render（免费，推荐）

1. 注册/登录 https://render.com
2. New + → **Web Service** → 选 **Deploy from Git**（先把 `relay/` 目录传到 GitHub 仓库）或选 **Build and deploy from a Git repository**
3. 配置：
   - Runtime: **Node**
   - Build Command: `npm install`
   - Start Command: `npm start`
4. 环境变量：
   - `GAME_ORIGIN` = `https://3dgunfight.app.workbuddy.host`
5. 创建后拿到地址，形如 `wss://gf-relay-xxxx.onrender.com`

## 部署到 Railway / Koyeb / Fly.io

同样思路：上传 `relay/` 目录，`npm install` + `npm start`，监听平台注入的 `PORT`，设置环境变量 `GAME_ORIGIN`。都支持 WebSocket。

## 部署到自己的轻量云服务器

```bash
# 服务器上（放行安全组端口，建议再用 caddy/nginx 挂 HTTPS）
GAME_ORIGIN=https://3dgunfight.app.workbuddy.host PORT=8787 node relay.js
```

## 最后一步：告诉游戏

拿到中继的 wss 地址后，把它填进 `public/js/net.js` 顶部：

```js
const WS_RELAY = 'wss://gf-relay-xxxx.onrender.com';
```

然后重新发布游戏即可。客户端会按 **直连 WS → 中继 WS → HTTP 降级** 的顺序自动尝试。
