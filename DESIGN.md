# 设计笔记

> 面向开发者/运维。只想部署看 [README.md](./README.md) 就够。

## 0. 设计哲学：测一次、信一次

最大的设计约束：

- **VPN Gate 公共节点本身不可信**：实测同一节点探测成功率约 1/3，抖动巨大
- **不能额外花一次探测**：免费额度（ip-api 45 次/分）有限
- **不能把失败的代价转给客户端**：超时就是失去一个用户

于是整个数据面围绕一个原则展开：**把「建连时本来就要付的那一次失败」当成唯一可用性信号**，而不是额外再跑探测。

---

## 1. 节点池加载 (`core/pool.js`)

唯一实现，Worker / Vercel / 本地自测共用。优先级：

```
SSTP_POOL  →  KV  →  POOL_URL  →  打包的 public/nodes.json
```

- 90 秒 TTL 记忆 + single-flight：热路径不重复拉外链，`fetch` 失败不阻塞建连
- 全链路都失败 → 返回上一份快照（`source: "stale"`），而不是空池
- 新鲜度看 `checked_at`（`poolAgeMinutes`），不是请求时刻

**为什么 `POOL_URL` 是 Vercel 的必选项**

Vercel 没有 KV，`public/nodes.json` 是构建产物。不配 `POOL_URL`，
Node 进程就把池永远冻结在上次部署那一刻。填 GitHub Pages 上定时任务
发布的快照地址：

```
https://<user>.github.io/<repo>/nodes.json
```

这样就能在不重部署的情况下拿到每 30 分钟刷新的数据。

---

## 2. 内容寻址的节点 ID

订阅里的 `?node=<nodeId>` 由 `nodeIdOf(host, port)` 生成：`n` + 11 位 hex。

早期版本用 **排序下标** —— 流水线每次重排，`node=2` 就漂移到另一个节点，
而客户端毫不知情。现在 ID 只跟 `host:port` 有关。

- 找不到 → 显式报错，而不是静默换节点
- `?node=<纯数字>` 仍接受，仅为兼容旧订阅（`parseNodePin` 优先判断纯数字）
- 取名加 `n` 前缀，因为 纯 hex 约有 0.35% 概率恰好全数字，会被误判成旧格式下标

---

## 3. 被动摘除 (`core/health.js`)

照搬 Envoy 的 `outlier_detection`，但删去「主动探测」：

| 事件 | 行为 |
| :--- | :--- |
| 建连失败一次 | 冷却 30s（±20% 抖动） |
| 再次失败 | 60s → 120s → 300s |
| 建连成功 | 立即清除 |
| 冷却中的超过 50% | 摘除失效，全部放回 |

状态仅 isolate / 进程内，不跨实例共享。它成本为零 —— 失败的那次拨号本来就付了。

---

## 4. 单调快照 (`poolRev` + `poolAgeMinutes`)

```
poolRev = 最新 checked_at : 节点数 : ID 集合哈希
```

pipeline 写 KV 前比对：

- 内容没变 → 跳过
- 新快照更旧 → **拒绝写入**，挡住重试任务把旧数据盖回来

客户端靠此判断是否需要刷新 `/data.json` / `/nodes.json`。

---

## 5. SSTP 链式代理

`core/sstp.js` 在纯 JS 里跑了完整 PPP 拨号 —— TLS → `SSTP_DUPLEX_POST` → LCP → PAP → IPCP → 用户态 TCP/IP 栈 —— 所以 Worker（`cloudflare:sockets`）和 Vercel（`ws` + Node `net`）都能跑，无需 TUN/TAP。

`core/upstream.js` 负责链式选点：

- `CHAIN=direct` → 一条直连
- `CHAIN=always` → 逐个试，最多 `SSTP_MAX_TRIES`（默认 3）
- `CHAIN=fallback` → 全部 SSTP 失败降级直连
- 选点时自动避开被动摘除中的节点；`?node=<id>` 则精确固定

---

## 6. early data 握手

VLESS over WS 的首包塞进 `Sec-WebSocket-Protocol` 头里。服务端必须：

1. 解析头里的 base64url 字段（跳过 `vless` 这种短协议名）
2. 回 `Sec-WebSocket-Protocol` 到 101，否则子协议协商失败

见 `core/test-early-data.mjs`。

---

## 7. 环境变量

| 变量 | 默认 | 说明 |
| :--- | :--- | :--- |
| `CHAIN` | `always` | `always \| fallback \| direct` |
| `SSTP_POOL` | | 逗号分隔 host:port，优先级最高 |
| `POOL_URL` | | GitHub Pages / 外链（Vercel 必填） |
| `POOL_CACHE_TTL_MS` | `90000` | 池缓存 TTL |
| `SSTP_MAX_TRIES` | `3` | 一次连接最多试几个节点 |
| `SSTP_USER` / `SSTP_PASS` | `vpn` / `vpn` | SSTP 凭据 |
| `PROBE_TARGET` | `ip-api.com` | `/check` 探测目标 |
| `SSTP_TIMEOUT_MS` | `12000` | 握手超时 |
| `PROBE_TIMEOUT_MS` | `15000` | 探测整体超时 |

鉴权：`UUID` / `ADMIN` 都是 **secret**，不要写进 `wrangler.toml` / `vercel env` 里。
