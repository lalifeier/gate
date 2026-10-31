// 平台无关的 HTTP 处理逻辑：Workers 与 Vercel(Node) 共用
// 返回 { status, headers, body:string }，由各平台适配层转成各自响应格式

import { sstpProbe } from './sstp.js';
import { classifyExit } from './classify.js';
import { projectNode, sortByLatency, groupByCountry, buildStats, attachComputed, VIEW_FIELDS } from './schema.js';
import { loadPool, loadPoolState, parsePoolSpec } from './pool.js';
import { isEjected, ejectedCount } from './health.js';

export const DEFAULTS = {
	CHAIN: 'always',          // always | fallback | direct
	SSTP_USER: 'vpn',
	SSTP_PASS: 'vpn',
	PROBE_TARGET: 'ip-api.com',
	SSTP_TIMEOUT_MS: 12000,
	PROBE_TIMEOUT_MS: 15000,
	// GitHub Pages 上由定时任务发布的节点池。Vercel 侧没有 KV，
	// 这是唯一能拿到「本轮检测结果」的来源。
	POOL_URL: '',
	POOL_CACHE_TTL_MS: 90000,
	// 用于订阅里的「连接地址」，解析到 CF 边缘 IP，但 SNI 仍指向你的域名。
	// ?cf=host 用内置列表，?cf=ip 用 CFHub，?cf_hosts=a,b 自定义
	CF_HOSTS: '',
};

export function readConfig(env = {}) {
	return {
		UUID: env.UUID || '',
		ADMIN: env.ADMIN || '',
		CHAIN: (env.CHAIN || DEFAULTS.CHAIN).toLowerCase(),
		SSTP_USER: env.SSTP_USER || DEFAULTS.SSTP_USER,
		SSTP_PASS: env.SSTP_PASS || DEFAULTS.SSTP_PASS,
		PROBE_TARGET: env.PROBE_TARGET || DEFAULTS.PROBE_TARGET,
		SSTP_TIMEOUT_MS: Number(env.SSTP_TIMEOUT_MS || DEFAULTS.SSTP_TIMEOUT_MS),
		PROBE_TIMEOUT_MS: Number(env.PROBE_TIMEOUT_MS || DEFAULTS.PROBE_TIMEOUT_MS),
		SSTP_POOL: env.SSTP_POOL || '',
		POOL_URL: env.POOL_URL || DEFAULTS.POOL_URL,
		POOL_CACHE_TTL_MS: Number(env.POOL_CACHE_TTL_MS || DEFAULTS.POOL_CACHE_TTL_MS),
		POOL_FETCH_TIMEOUT_MS: Number(env.POOL_FETCH_TIMEOUT_MS || 4000),
		EDGE_HOSTS: env.EDGE_HOSTS || '',
		NODE_HOST: env.NODE_HOST || '',
		CF_HOSTS: env.CF_HOSTS || '',
	};
}

/**
 * 内置的 CF 优选域名。
 *
 * 这些域名本身部署在 Cloudflare 上，客户端连它们等于连到 CF 边缘；
 * 配合 sni/host 指向自己的 Worker 域名，就能绕开被污染的入口 IP。
 */
export const CF_HOSTS = [
	'www.visa.com.hk', 'www.visa.cn', 'www.mastercard.com.hk', 'www.paypal.com',
	'ip.sb', 'time.is', 'speed.cloudflare.com', 'www.udemy.com',
	'www.shopify.com', 'www.vimeo.com', 'www.digitalocean.com',
	'icook.tw', 'www.skyscanner.jp', 'singapore.com', 'japan.com', 'www.hktv.com.hk',
	'dash.cloudflare.com', 'workers.dev', 'pages.dev', 'cdnjs.cloudflare.com',
	'www.who.int', 'www.un.org', 'www.csgo.com', 'update.microsoft.com', 'gateway.icloud.com',
	'ip.164746.xyz', 'cdn.2020111.xyz', 'bestcf.top', 'cfip.cfcdn.vip', 'freeyx.cloudflare88.eu.org',
	'cfip.xxxxxxxx.tk', 'saas.sin.fan', 'cf.090227.xyz', 'cloudflare.182682.xyz', 'bestcf.030101.xyz',
];

/**
 * 从 CFHub 拉优选 IP（IPv4）。
 * CFHub 站群众包探测得到的到 CF 边缘 IP 的延迟+投票，按 `isp` 分池。
 *
 * 拉取结果以 60s 缓存 + single-flight 复用；Worker 启动时就拉一次，
 * 订阅接口直接读缓存，不在热路径内发网络请求。
 */
const CFHUB_URL = 'https://cfhub.1molchuan.top/api/v1/pools';
const MAX_SUB_LINES = 200;  // 订阅单次输出上限，防止节点 × 优选地址组合爆炸
let cfIpsMemo = null;       // { ips: [...] | null, at: number }
let cfIpsInflight = null;
const CF_TTL_MS = 60_000;

async function fetchCfIps() {
	const now = Date.now();
	if (cfIpsMemo && now - cfIpsMemo.at < CF_TTL_MS) return cfIpsMemo.ips;
	if (cfIpsInflight) return cfIpsInflight;
	cfIpsInflight = (async () => {
		try {
			const r = await fetch(CFHUB_URL, { cf: { cacheTtl: 60 }, redirect: 'follow' });
			if (!r.ok) throw new Error(`HTTP ${r.status}`);
			const j = await r.json();
			// 只拿 v4、published 的 IP
			const ips = j.pools
				.filter(p => p.family === 4 && p.published && p.ips?.length)
				.flatMap(p => p.ips.map(i => ({ ip: i.ip, median_ms: i.median_ms || 0, votes: i.votes || 0, isp: p.isp })));
			// 按投票降序
			ips.sort((a, b) => b.votes - a.votes);
			cfIpsMemo = { ips, at: now };
			return ips;
		} catch (e) {
			// 拉失败也别把缓存清了，保底用上一轮
			return cfIpsMemo?.ips || [];
		} finally {
			cfIpsInflight = null;
		}
	})();
	return cfIpsInflight;
}

// /api/cf-ips：暴露优选 IP 列表给客户端脚本用
async function handleCfIps({ query }) {
	const ips = await fetchCfIps();
	const isp = query.get('isp');
	const out = isp ? ips.filter(i => i.isp === isp) : ips;
	return json({ ips: out, cached_at: cfIpsMemo?.at || null });
}

const json = (obj, status = 200) => ({
	status,
	headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
	body: JSON.stringify(obj, null, 1),
});

const text = (s, status = 200, headers = {}) => ({
	status,
	headers: { 'content-type': 'text/plain; charset=utf-8', ...headers },
	body: s,
});

const html = (s, status = 200) => ({
	status,
	headers: { 'content-type': 'text/html; charset=utf-8' },
	body: s,
});

/**
 * 主路由
 * @param {object} p
 * @param {string} p.pathname
 * @param {URLSearchParams} p.query
 * @param {string} p.origin
 * @param {string} p.hostname
 * @param {object} p.env
 * @param {Function} p.dial     (host, port, opts) => socket 形状
 * @param {Function} p.resolve  (host) => ipv4
 * @param {object} p.cf         Cloudflare request.cf（可为 null）
 */
export async function routeHttp({ pathname, query, origin, hostname, env, dial, resolve, cf = null }) {
	const cfg = readConfig(env);

	if (pathname === '/check') return handleCheck({ query, env, cfg, dial, resolve, cf });
	if (pathname === '/nodes.txt') return handleNodesTxt({ env, cfg });
	if (pathname === '/api/nodes') return handleApiNodes({ env, cfg });
	if (pathname === '/api/cf-ips') return handleCfIps({ query });
	if (pathname === '/sub') return handleSub({ query, env, cfg, hostname, origin });
	if (pathname === '/admin') return handleAdmin({ query, env, cfg, origin });
	if (pathname === '/healthz') return handleHealthz({ env, cfg });

	// 这三个都动态生成，不走静态资源缓存：
	// 定时任务刷新后，缓存住就会返回旧数据甚至旧结构
	if (pathname === '/data.json') return handleDataJson({ env, cfg });
	if (pathname === '/nodes.json') return handleNodesJson({ env, cfg });
	if (pathname === '/' || pathname === '/index.html') return handleShowcase({ env, origin });

	return handleIndex({ origin, cfg });
}

// 静态资源缺失时的兜底首页
export async function fallbackIndex({ origin, env }) {
	return handleIndex({ origin, cfg: readConfig(env) });
}

// ---------------------------------------------------------------------------
// /check：SSTP 节点可用性 + 出口识别
// ---------------------------------------------------------------------------
async function handleCheck({ query, env, cfg, dial, resolve, cf }) {
	const raw = query.get('sstp');
	if (!raw) return json({ success: false, error: 'missing ?sstp=[user:pass@]host:port' }, 400);

	const m = /^(?:sstp:\/\/)?(?:([^:@/]+):([^@/]*)@)?([^:/]+)(?::(\d+))?$/i.exec(raw.trim());
	if (!m) return json({ success: false, error: 'invalid sstp spec' }, 400);

	const [, user = cfg.SSTP_USER, pass = cfg.SSTP_PASS, host, portStr] = m;
	const port = Number(portStr || 443);
	if (!host || !(port >= 1 && port <= 65535)) return json({ success: false, error: 'invalid host/port' }, 400);

	const started = Date.now();

	// VPN Gate 公共节点抖动明显，单次失败不代表不可用，最多试 2 次
	let probe = null, lastErr = null;
	for (let attempt = 0; attempt < 2 && !probe; attempt++) {
		try {
			const r = await sstpProbe({
				host, port, username: user, password: pass,
				dial, resolve,
				targetHost: cfg.PROBE_TARGET, targetPort: 80,
				path: '/json/?fields=status,country,countryCode,city,isp,org,as,query',
				timeout: cfg.PROBE_TIMEOUT_MS,
			});
			if (r.success) probe = r;
			else lastErr = new Error(r.status_line || 'no response');
		} catch (e) {
			lastErr = e;
		}
	}

	if (!probe) {
		return json({
			success: false,
			responseTime: Date.now() - started,
			colo: cf?.colo || null,
			error: lastErr?.message || String(lastErr),
		});
	}

	try {
		let exit = null;
		try {
			const b = JSON.parse(probe.body);
			if (b?.query) {
				exit = {
					ip: b.query,
					country: b.country,
					country_code: b.countryCode,
					city: b.city,
					asn: { org: b.isp || b.org || '', asn: b.as || '' },
					org: b.isp || b.org || '',
				};
			}
		} catch (e) { /* 非 JSON */ }

		// 出口 ASN 二次查询：SSTP 隧道内看到的 isp 往往是 SoftEther 本身，
		// 真正决定住宅/机房的是出口 IP 归属。
		if (exit?.ip) {
			const asnInfo = await lookupExitAsn(exit.ip);
			if (asnInfo) {
				exit.asn = { org: asnInfo.org || asnInfo.isp || '', asn: asnInfo.as || '' };
				exit.org = asnInfo.isp || asnInfo.org || exit.org;
				exit.country = asnInfo.country || exit.country;
				exit.country_code = asnInfo.countryCode || exit.country_code;
				exit.city = asnInfo.city || exit.city;
				exit.type = classifyExit(exit.org);
			}
		}

		return json({
			success: !!probe.success && !!exit,
			responseTime: Date.now() - started,
			colo: cf?.colo || null,
			assigned_ip: probe.assigned_ip || null,
			bytes: probe.bytes,
			exit,
			error: probe.success ? (exit ? null : 'no exit info') : (probe.status_line || 'no response'),
		});
	} catch (e) {
		return json({
			success: false,
			responseTime: Date.now() - started,
			colo: cf?.colo || null,
			error: e?.message || String(e),
		});
	}
}

// 节点池读写的唯一实现在 core/pool.js，这里只做 thin wrapper。
export function parsePool(cfg) {
	return parsePoolSpec(cfg.SSTP_POOL);
}

async function handleNodesTxt({ env, cfg }) {
	const pool = await loadPool(env, cfg);
	const edge = cfg.EDGE_HOSTS.split(',').map(s => s.trim()).filter(Boolean);
	const lines = pool.map((n, i) => {
		const entry = edge.length ? edge[i % edge.length] : `${n.host}:${n.port}`;
		const tag = n.tag || `${n.code || 'SSTP'}-${String(i + 1).padStart(2, '0')}`;
		return `${entry}#${tag}$sstp://${cfg.SSTP_USER}:${cfg.SSTP_PASS}@${n.host}:${n.port}`;
	});
	return text(lines.join('\n') + (lines.length ? '\n' : ''), 200, { 'cache-control': 'public, max-age=300' });
}

/** /api/nodes：带池元数据的 JSON，调用方可据此判断是否需要刷新 */
async function handleApiNodes({ env, cfg }) {
	const { pool, rev, age_minutes, source } = await loadPoolState(env, cfg);
	return json({
		generated_at: new Date().toISOString(),
		pool_rev: rev,
		age_minutes,
		source,
		count: pool.length,
		ejected: ejectedCount(),
		nodes: pool,
	});
}

/** /healthz：池状态 + 被动摘除计数，用于观测与告警 */
async function handleHealthz({ env, cfg }) {
	const { pool, rev, age_minutes, source } = await loadPoolState(env, cfg);
	return json({
		ok: true,
		ts: Date.now(),
		chain: cfg.CHAIN,
		pool_rev: rev,
		age_minutes,
		source,
		pool_size: pool.length,
		ejected: ejectedCount(),
	});
}

// /nodes.json：节点池纯数组（与目录结构一致），no-store
async function handleNodesJson({ env, cfg }) {
	const pool = await loadPool(env, cfg);
	return {
		status: 200,
		headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
		body: JSON.stringify(pool, null, 1),
	};
}

// /data.json：用节点池现场生成，结构与 web/index.html 对齐。
// 动态生成 + no-store，避免边缘缓存返回旧结构。
async function handleDataJson({ env, cfg }) {
	const { pool, rev, age_minutes, source } = await loadPoolState(env, cfg);

	// 字段由 core/schema.js 统一定义，与 pipeline 生成的 data.json 同构。
	const viewNodes = pool.map(n => attachComputed(projectNode(n, VIEW_FIELDS), {
		sstpUser: cfg.SSTP_USER,
		sstpPass: cfg.SSTP_PASS,
	}));

	const data = {
		generated_at: new Date().toISOString(),
		source: 'vpngate-edge',
		pool_rev: rev,
		age_minutes,
		pool_source: source,
		stats: buildStats(viewNodes, { checked: pool.length }),
		countries: groupByCountry(viewNodes),
		available: sortByLatency(viewNodes),
	};

	return {
		status: 200,
		headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
		body: JSON.stringify(data, null, 1),
	};
}

// /：展示页（优先用 ASSETS 里的 index.html，缺失则用内置模板）
async function handleShowcase({ env, origin }) {
	let htmlText = null;
	if (env.ASSETS) {
		try {
			const res = await env.ASSETS.fetch(new Request(new URL('/index.html', origin || 'https://x/'), { method: 'GET' }));
			if (res.ok) {
				const t = await res.text();
				if (t && !/^\s*$/.test(t)) htmlText = t;
			}
		} catch (e) { /* 回落 */ }
	}
	if (!htmlText) htmlText = BUILTIN_PAGE;
	return {
		status: 200,
		headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' },
		body: htmlText,
	};
}

// 内置展示页兜底：web/index.html 的副本（无 ASSETS 绑定时的降级）
const BUILTIN_PAGE = `<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VPN Gate \u00b7 SSTP \u8fb9\u7f18\u51fa\u53e3</title>
<style>
body{background:#0b1120;color:#dbe7ff;font-family:system-ui,sans-serif;padding:40px;line-height:1.7}
h2{margin-bottom:6px}a{color:#4cc2ff}
code{background:#16233c;padding:2px 7px;border-radius:5px;font-size:13px}
.muted{color:#7f93b8;font-size:13px}
</style></head><body>
<h2>VPN Gate \u00b7 SSTP \u8fb9\u7f18\u51fa\u53e3</h2>
<p class="muted">\u670d\u52a1\u8fd0\u884c\u4e2d\u3002\u5b8c\u6574\u5c55\u793a\u9875\u89c1 <code>/admin?token=ADMIN</code>\uff0c\u8282\u70b9\u5217\u8868\u89c1 <code>/data.json</code>\u3002</p>
<ul>
<li>WebSocket \u5165\u53e3\uff1a<code>wss://\u672c\u57df\u540d/</code></li>
<li>\u8282\u70b9\u6c60\uff1a<a href="/api/nodes">/api/nodes</a> \u00b7 <a href="/nodes.txt">/nodes.txt</a></li>
<li>\u5b9e\u65f6\u6570\u636e\uff1a<a href="/data.json">/data.json</a></li>
<li>\u8ba2\u9605\uff1a<code>/sub?token=ADMIN</code>\u3000\u540e\u53f0\uff1a<code>/admin?token=ADMIN</code></li>
</ul>
</body></html>`;

// ---------------------------------------------------------------------------
// 后台 / 订阅 / 首页
// ---------------------------------------------------------------------------
async function handleAdmin({ query, env, cfg, origin }) {
	if (!cfg.ADMIN || query.get('token') !== cfg.ADMIN) return text('forbidden', 403);

	const pool = await loadPool(env, cfg);
	const rows = pool.map((n, i) => {
		const t = n.resident || 'unknown';
		const badge = t === 'residential' ? '住宅' : t === 'datacenter' ? '机房' : '未知';
		return `<tr><td>${i + 1}</td><td>${esc(n.host)}</td><td>${n.port}</td>`
			+ `<td>${esc(n.country || '')}</td><td>${badge}</td>`
			+ `<td>${n.latency_ms != null ? n.latency_ms + ' ms' : '-'}</td></tr>`;
	}).join('');

	const resCount = pool.filter(n => n.resident === 'residential').length;
	const dcCount = pool.filter(n => n.resident === 'datacenter').length;

	// CFHub 优选 IP 预览（拉不到就跳过，不影响后台其余部分）
	const cfIps = await fetchCfIps().catch(() => []);
	const cfRows = cfIps.slice(0, 10).map(i =>
		`<tr><td>${esc(i.ip)}</td><td>${esc(i.isp)}</td><td>${i.median_ms || '-'} ms</td><td>${i.votes || 0}</td></tr>`,
	).join('');
	const cfSection = cfIps.length ? `
<h2 style="margin-top:24px">CF 优选 IP（CFHub，${cfIps.length} 个）</h2>
<p class="muted">订阅 <code>?cf=ip</code> 会自动使用这批 IP；<code>?cf=ip&amp;isp=chinanet</code> 按运营商筛选。<br>
原始数据 <code>${esc(origin)}/api/cf-ips</code></p>
<table><tr><th>IP</th><th>线路</th><th>中位延迟</th><th>投票</th></tr>${cfRows}</table>`
		: '<p class="muted" style="margin-top:24px">CFHub 优选 IP 暂不可用（拉取失败），订阅会用真实域名。</p>';

	return html(`<!doctype html><meta charset="utf-8"><title>vpngate 后台</title>
<style>
body{font-family:system-ui;background:#0b1120;color:#dbe7ff;padding:24px;line-height:1.6}
table{border-collapse:collapse;width:100%;max-width:1000px;margin-top:12px}
td,th{border-bottom:1px solid #243450;padding:8px;text-align:left;font-size:13px}
th{color:#7f93b8;font-weight:500}
code{background:#16233c;padding:2px 6px;border-radius:4px;font-size:12px}
h2{margin-bottom:6px}.muted{color:#7f93b8;font-size:13px}
</style>
<h2>节点池（${pool.length}）</h2>
<p class="muted">
链式模式 <b>${esc(cfg.CHAIN)}</b>　住宅 ${resCount} · 机房 ${dcCount} · 未知 ${pool.length - resCount - dcCount}<br>
订阅 <code>${esc(origin)}/sub?token=***</code>（<code>?mode=auto&amp;cf=host</code> 自动优选）<br>
检测 <code>${esc(origin)}/check?sstp=vpn:vpn@host:port</code><br>
节点 JSON <code>${esc(origin)}/api/nodes</code>
订阅行 <code>${esc(origin)}/nodes.txt</code>
</p>
<table><tr><th>#</th><th>Host</th><th>Port</th><th>国家</th><th>类型</th><th>延迟</th></tr>
${rows || '<tr><td colspan="6">空（先跑 pipeline/check.mjs 生成节点池）</td></tr>'}</table>
${cfSection}`);
}

// 简易落地页（/admin 之外的根路径）
async function handleIndex({ origin, cfg }) {
	return html(`<!doctype html><meta charset="utf-8"><title>vpngate edge</title>
<style>
body{font-family:system-ui;background:#0b1120;color:#dbe7ff;padding:40px;line-height:1.8}
a{color:#4cc2ff}code{background:#16233c;padding:2px 7px;border-radius:5px;font-size:13px}
.muted{color:#7f93b8;font-size:13px}
</style>
<h2>VPN Gate · SSTP 边缘出口</h2>
<p class="muted">链式模式 <b>${esc(cfg.CHAIN)}</b></p>
<ul>
<li>WebSocket 入口：<code>wss://本域名/</code>（VLESS + WS + TLS）</li>
<li>展示页：<a href="/">/</a>　实时数据：<a href="/data.json">/data.json</a></li>
<li>节点池：<a href="/api/nodes">/api/nodes</a> · <a href="/nodes.txt">/nodes.txt</a> · <a href="/nodes.json">/nodes.json</a></li>
<li>检测：<a href="/check?sstp=vpn:vpn@127.0.0.1:443">/check?sstp=vpn:vpn@host:port</a></li>
<li>后台：<code>/admin?token=ADMIN</code>　订阅：<code>/sub?token=ADMIN</code></li>
</ul>`);
}

function esc(s) {
	return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function b64(s) {
	if (typeof btoa === 'function') return btoa(unescape(encodeURIComponent(s)));
	return Buffer.from(s, 'utf8').toString('base64');
}

/**
 * 订阅。
 *
 *   ?mode=multi   （默认）每节点一条，路径带 ?node=<id> 固定节点
 *   ?mode=single  每个入口一条，后端自动轮换节点
 *   ?mode=auto    每个入口一条，后端按「住宅优先 + 延迟升序 + 非冷却」自动选优
 *
 *   &format=text  输出明文；默认 base64
 *   &limit=N      最多取 N 个节点（仅 multi）
 *   &node=1,2,3   只输出指定节点（内容寻址 ID）
 *
 *   连接地址（sni/host 始终是真实 Worker 域名，仅换 connect address）：
 *   &cf=host       内置 CF 优选域名
 *   &cf=ip         CFHub 众包优选 IP
 *   &cf_hosts=a,b  自定义域名/IP
 *   &isp=chinanet  配合 cf=ip 按运营商筛选
 *   &cf_limit=N    取几个优选地址（multi 默认 1，auto/single 默认 8）
 */
async function handleSub({ query, env, cfg, hostname, origin }) {
	if (!cfg.ADMIN || query.get('token') !== cfg.ADMIN) return text('forbidden', 403);
	if (!cfg.UUID) return text('UUID not configured', 500);

	// sni / ws host 始终是真实域名，否则 CF 无法按 SNI 路由到本 Worker
	const realHosts = (cfg.NODE_HOST || hostname).split(',').map(s => s.trim()).filter(Boolean);
	const realHost = realHosts[0] || hostname;

	const mode = (query.get('mode') || 'multi').toLowerCase();
	const limit = Number(query.get('limit') || 0);

	// 连接地址：&cf=host 用内置优选域名；&cf=ip 用 CFHub 的优选 IP；&cf_hosts=a,b 自定义
	const cfCustom = query.get('cf_hosts');
	const cfMode = (query.get('cf') || '').toLowerCase();
	// multi 会把「节点 × 地址」展开，默认只取 1 个地址；auto/single 一条链接，默认取 8 个
	const cfLimitDefault = mode === 'multi' ? 1 : 8;
	let cfHosts = null;
	if (cfCustom != null) {
		cfHosts = cfCustom.split(',').map(s => s.trim()).filter(Boolean);
	} else if (cfMode === 'ip') {
		const ips = await fetchCfIps();
		const isp = (query.get('isp') || '').toLowerCase();
		const picked = isp ? ips.filter(i => i.isp === isp) : ips;
		// 空数组是 truthy，必须显式转 null，否则下游 targets 会是空列表
		const hosts = picked.slice(0, Number(query.get('cf_limit') || cfLimitDefault)).map(i => i.ip);
		cfHosts = hosts.length ? hosts : null;
	} else if (cfMode) {
		const all = cfg.CF_HOSTS ? cfg.CF_HOSTS.split(',').map(s => s.trim()).filter(Boolean) : CF_HOSTS;
		cfHosts = all.slice(0, Number(query.get('cf_limit') || cfLimitDefault));
	}

	// 自动选优：住宅优先 → 延迟升序 → 非冷却
	const pickBest = (pool) => {
		const live = pool.filter(n => !isEjected(n));
		const cand = live.length ? live : pool;
		return [...cand].sort((a, b) => {
			const ar = a.resident === 'residential' ? 0 : 1;
			const br = b.resident === 'residential' ? 0 : 1;
			if (ar !== br) return ar - br;
			return (a.latency_ms ?? 1e9) - (b.latency_ms ?? 1e9);
		})[0] || null;
	};

	let entries = [];
	if (mode === 'single' || mode === 'auto') {
		const pool = await loadPool(env, cfg);
		const best = mode === 'auto' && pool.length ? pickBest(pool) : null;
		const path = best ? `/?ed=2048&node=${encodeURIComponent(best.nodeId || `${best.host}:${best.port}`)}` : '/?ed=2048';
		const cc = best ? (best.code || 'XX').toUpperCase() : 'EDGE';
		const kind = best?.resident === 'residential' ? '住宅' : best?.resident === 'datacenter' ? '机房' : 'SSTP';
		const name = mode === 'auto' && best ? `${cc}-${kind}-优选` : null;
		const targets = cfHosts || realHosts;
		entries = targets.map((h, i) => ({
			connectHost: h,
			sniHost: realHost,
			name: name || `EDGE-SSTP-${String(i + 1).padStart(2, '0')}`,
			path,
		}));
	} else {
		const pool = await loadPool(env, cfg);
		const picked = limit > 0 ? pool.slice(0, limit) : pool;
		if (!picked.length) {
			const targets = cfHosts || realHosts;
			entries = targets.map((h, i) => ({
				connectHost: h,
				sniHost: realHost,
				name: `EDGE-SSTP-${String(i + 1).padStart(2, '0')}`,
				path: '/?ed=2048',
			}));
		} else {
			// 节点 × 优选地址会爆炸：按 limit 取节点，按 cf_limit 取地址，
			// 且组合总数封顶，避免一次返回几百条。
			const targets = cfHosts || realHosts;
			entries = picked.flatMap((n, i) => targets.map(h => {
				const cc = (n.code || 'XX').toUpperCase();
				const kind = n.resident === 'residential' ? '住宅' : n.resident === 'datacenter' ? '机房' : 'SSTP';
				return {
					connectHost: h,
					sniHost: realHost,
					name: [cc, kind, String(i + 1).padStart(2, '0')].join('-'),
					path: `/?ed=2048&node=${encodeURIComponent(n.nodeId || `${n.host}:${n.port}`)}`,
				};
			})).slice(0, MAX_SUB_LINES);
		}
	}

	const lines = entries.map(({ connectHost, sniHost, name, path }, i) => {
		// sni/host 始终写真实域名：优选域名只作为「连接地址」，
		// CF 靠 SNI 判断请求该路由到哪个 Worker。
		const host = connectHost !== realHost ? sniHost : connectHost;
		// 同一节点配多个优选域名时，标签需要唯一，否则客户端会按名字去重
		const suffix = connectHost !== realHost && (cfHosts || realHosts).length > 1 ? `@${connectHost}` : '';
		return `vless://${cfg.UUID}@${connectHost}:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=${encodeURIComponent(path)}#${encodeURIComponent(name + suffix)}`;
	});

	const body = lines.join('\n');
	const headers = { 'cache-control': 'no-store' };
	// cf=ip 但 CFHub 拉不到时，链接回落到真实域名；用响应头告诉客户端
	if (cfMode === 'ip' && !cfHosts?.length) headers['x-cf-ip-fallback'] = '1';
	if (query.get('format') === 'text') return text(body, 200, headers);
	return text(b64(body), 200, headers);
}

// ---------------------------------------------------------------------------
// 出口 ASN 二次查询
// ---------------------------------------------------------------------------
// SSTP 隧道内看到的 isp 往往就是 SoftEther 本身，真正决定住宅/机房的是出口 IP 归属。
// 带内存缓存，避免同一 IP 反复查（ip-api 免费额度 45 次/分钟）。
const asnCache = new Map();
const ASN_TTL_MS = 3600_000;

async function lookupExitAsn(ip) {
	if (!ip) return null;

	const hit = asnCache.get(ip);
	if (hit && Date.now() - hit.at < ASN_TTL_MS) return hit.data;

	try {
		const res = await fetch(
			`http://ip-api.com/json/${encodeURIComponent(ip)}?fields=status,country,countryCode,city,isp,org,as`,
			{ signal: AbortSignal.timeout(5000) },
		);
		const j = await res.json();
		if (!j || j.status !== 'success') return null;
		asnCache.set(ip, { at: Date.now(), data: j });
		return j;
	} catch (e) {
		return null;
	}
}
