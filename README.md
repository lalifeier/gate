# VPN Gate · SSTP 家宽出口

用 **VLESS + WebSocket** 做入口，把流量从 Cloudflare Worker / Vercel 的出口**链式转进 VPN Gate 的 SSTP 节点**，落地是住宅 / 家宽 IP。

- 入口：`VLESS over WebSocket`（TLS 由平台提供）
- 出口：`SSTP`（纯 JS 用户态 PPP + TCP/IP 栈）
- 节点：VPN Gate 公开列表，GitHub Actions 每 30 分钟抓取 + 探测 + 发布
- 同一份核心代码跑在 Cloudflare Workers 与 Vercel Functions

> 设计取舍、协议细节、为什么这么做 → [DESIGN.md](./DESIGN.md)

---

## 快速开始

```bash
npm install

# 只测路由与 WS 转发（出口直连）
node core/selftest.mjs

# 测真实 SSTP 链路（本机通常需要代理才能连到 VPN Gate）
DIAL_PROXY=http://127.0.0.1:1080 node core/selftest.mjs --sstp

# 完整测试
npm test
```

---

## 部署到 Cloudflare Workers

```bash
cp .env.example .env            # 填 UUID / ADMIN
npx wrangler secret put UUID
npx wrangler secret put ADMIN
npx wrangler deploy
```

`wrangler.toml` 的 `[assets]` 已指向 `./public`。三个容易踩的点：

- 必须是 `[assets]`（表），不是 `[[assets]]`
- 必须 `run_worker_first = true`，否则静态资源会吃掉 WebSocket 升级请求
- 必须 `html_handling = "none"`，否则 `/index.html` 会被 307 回 `/`，浏览器陷入重定向循环

无 wrangler 环境：

```bash
CF_API_TOKEN=... CF_ACCOUNT_ID=... UUID=... ADMIN=... \
  node scripts/deploy-worker.mjs
```

自定义域名：Workers → Triggers → Add Custom Domain。

---

## 部署到 Vercel

```bash
vercel link
vercel env add UUID
vercel env add ADMIN
vercel env add CHAIN         # always / fallback / direct
vercel env add POOL_URL      # 必填，见下
vercel --prod
```

**`POOL_URL` 是 Vercel 的必填项**，填 GitHub Pages 上定时任务发布的快照：

```
https://<user>.github.io/<repo>/nodes.json
```

不填的话节点池会冻结在上次部署（`public/nodes.json` 是构建产物）。

`vercel.json` 已设 `maxDuration: 300`；WebSocket 连接到期后客户端自动重连。

---

## 定时任务与 GitHub Pages

`.github/workflows/check.yml`，每 30 分钟：

1. 抓 VPN Gate → 筛 SSTP → 并发探测
2. 产出 `public/nodes.json` / `nodes.txt` / `data.json`
3. 配了 `CF_*` 就同时写 Worker 的 KV
4. 发布到 GitHub Pages

**Pages 需要手动开启一次**：仓库 Settings → Pages → Source 选 `GitHub Actions`。
默认的 `GITHUB_TOKEN` 没有 `repo` scope，Action 无法自己创建 Pages 站点。

GitHub Runner 到 VPN Gate 常常不通，用 `DIAL_PROXY` Secret 指向自己的 HTTP 代理。

### 手动跑流水线

```bash
node pipeline/check.mjs --no-probe              # 只抓取筛选
DIAL_PROXY=http://127.0.0.1:1080 \
  node pipeline/check.mjs --limit 24 --timeout 30000 --concurrency 12
```

`latency_ms` 不是 ping，是一次完整探测的墙钟耗时（TLS → SSTP 拨号 → 隧道内 HTTP）。
用 `CHECK_WORKER` 让探测从 CF 边缘出发，数值才反映客户端真实路径。

---

## 客户端

访问 `https://你的域名/sub?token=ADMIN` 拿到 VLESS 链接，导入 v2rayN / sing-box / Clash.Meta：

```text
vless://<UUID>@edt.example.com:443
  ?encryption=none&security=tls&sni=edt.example.com&fp=chrome
  &type=ws&host=edt.example.com
  &path=%2F%3Fed%3D2048%26node%3Dn376f414b5e2#KR-住宅-01
```

- `ed=2048` 开启 early data，必须支持（否则连上没响应）
- `node=<nodeId>` 固定到某个节点，ID 内容寻址，不会随排序漂移

### 订阅模式

| 参数 | 效果 |
| :--- | :--- |
| `?mode=multi`（默认） | 每个节点一条链接，各自固定 `?node=<id>` |
| `?mode=single` | 每个入口一条，后端自动轮换节点 |
| `?mode=auto` | 一条链接，后端按「住宅优先 → 延迟升序 → 非冷却」选最优 |
| `&cf=host` | 连接地址换成内置 CF 优选域名（35 个备选） |
| `&cf=ip` | 连接地址换成 CFHub 众包优选 IP |
| `&cf_hosts=a.com,1.2.3.4` | 自定义优选域名/IP 列表 |
| `&isp=chinanet` | 配合 `cf=ip`，按运营商（电信/联通/移动/教育网/云厂商）筛选 |
| `&cf_limit=N` | 取几个优选地址。`multi` 默认 1，`auto`/`single` 默认 8 |

CF 优选只替换 **连接地址**，`sni` 与 ws `host` 仍是你的真实域名，
这样 CF 能按 SNI 把请求路由到你的 Worker：

```text
vless://<UUID>@www.visa.com.hk:443
  ?security=tls&sni=edt.example.com&type=ws&host=edt.example.com
  &path=/?ed=2048&node=n376f414b5e2#JP-住宅-优选@www.visa.com.hk
```

### CFHub 优选 IP

`/api/cf-ips` 代理 [cfhub](https://github.com/1molchuan/cfhub) 的众包数据，
Worker 侧缓存 60 秒：

```bash
curl "https://你的域名/api/cf-ips"                 # 全部
curl "https://你的域名/api/cf-ips?isp=chinanet"    # 只电信
```

返回按投票降序的 `{ ip, median_ms, votes, isp }`。`/sub?cf=ip` 直接用它。
CFHub 不可达时订阅回落真实域名，并带 `x-cf-ip-fallback: 1` 响应头。
后台 `/admin` 也会展示前 10 个。

---

## HTTP 接口

| 路径 | 说明 |
| :--- | :--- |
| `WS /` | VLESS over WebSocket 数据入口 |
| `GET /sub?token=ADMIN` | VLESS 订阅。参数：<br> `?mode=auto` 自动选最优节点<br> `?cf=host` 用 CF 优选域名作为连接地址<br> `&format=text` 明文，默认 base64 |
| `GET /admin?token=ADMIN` | 节点池后台 |
| `GET /check?sstp=vpn:vpn@host:port` | 单节点探测 |
| `GET /api/nodes` | 节点池 JSON + `pool_rev` / `age_minutes` |
| `GET /api/cf-ips` | CFHub 优选 IP 列表（`?isp=chinanet` 可筛选） |
| `GET /nodes.txt` | 订阅行格式 |
| `GET /nodes.json` | 节点池纯数组 |
| `GET /data.json` | 展示页数据 |
| `GET /healthz` | 池状态（`source` / `age_minutes` / `ejected`） |

`/data.json`、`/nodes.json`、`/` 都是动态生成 + `no-store`，避免边缘缓存旧结构。

---

## 常见问题

**全部节点探测失败** — 网络到 `*.opengw.net:443` 被阻断。配 `DIAL_PROXY`，或把流水线放到能出海的机器。

**客户端连上了但打不开网页** — 先 `/check?sstp=vpn:vpn@节点:端口` 确认 `success: true`；再看 `/admin` 池是否为空；用 `CHAIN=fallback` 区分入口问题还是出口问题。

**SSTP 握手 / PAP 失败** — 公共节点凭据是 `vpn`/`vpn`。部分节点不是 443 端口，池里已带端口。

**单个节点时好时坏** — 公共节点抖动大（成功率约 1/3）。`/check` 重试一次、建连最多试 3 个节点、被动摘除会跳过刚失败的。

**Vercel 几分钟后断开** — WebSocket 随 `maxDuration` 关闭，客户端重连即可。

---

## 安全

- VPN Gate 是公共中转，不要传输敏感凭据；SSTP 的 TLS 只保护到节点这一段
- `ADMIN` 用高强度随机串，`/admin` 与 `/sub` 都靠它
- 不要把 `.env`、`CF_API_TOKEN`、KV ID 提交进仓库

---

## 致谢

| 项目 | 用途 |
| :--- | :--- |
| [VPN Gate](https://www.vpngate.net/) | 节点数据源 |
| [fdciabdul/Vpngate-Scraper-API](https://github.com/fdciabdul/Vpngate-Scraper-API) | 官方 API 失效时的镜像 |
| [1molchuan/cfhub](https://github.com/1molchuan/cfhub) | 众包 CF 优选 IP 数据（`/api/cf-ips`） |
