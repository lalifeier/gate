// Cloudflare Workers 入口
//
// 路由：
//   GET /check?sstp=vpn:vpn@host:port   节点检测（含出口识别）
//   GET /api/nodes  /nodes.txt          节点池
//   GET /admin?token=ADMIN              后台
//   GET /sub?token=ADMIN                订阅
//   GET /  (Upgrade: websocket)         VLESS over WS -> SSTP 链式出口
//
// 绑定/变量：UUID ADMIN CHAIN SSTP_POOL SSTP_USER SSTP_PASS PROBE_TARGET POOL_URL KV

import { createCfDial, resolveA } from '../../core/dial-cf.js';
import { routeHttp, readConfig } from '../../core/router.js';
import { loadPool } from '../../core/pool.js';
import { createVlessSession } from '../../core/vless-ws.js';
import { createUpstreamOpener, parseNodePin } from '../../core/upstream.js';

const dial = createCfDial();

export default {
	async fetch(request, env) {
		const url = new URL(request.url);
		const upgrade = (request.headers.get('Upgrade') || '').toLowerCase();

		if (upgrade === 'websocket') {
			return handleWs(request, env, url);
		}

		const res = await routeHttp({
			pathname: url.pathname,
			query: url.searchParams,
			origin: url.origin,
			hostname: url.hostname,
			env: withStaticReader(env, url.origin),
			dial,
			resolve: resolveA,
			cf: request.cf || null,
		});

		return new Response(res.body, { status: res.status, headers: res.headers });
	},
};

// 给 router 注入静态读取能力：优先 ASSETS；KV 由 router 内部处理
function withStaticReader(env, origin) {
	if (!env.ASSETS) return env;
	if (typeof env.__readStatic === 'function') return env;
	return {
		...env,
		__readStatic: async (name) => {
			const res = await env.ASSETS.fetch(new Request(new URL('/' + name, origin), { method: 'GET' }));
			if (!res.ok) throw new Error('asset 404: ' + name);
			return res.text();
		},
	};
}

/**
 * 解析 WebSocket 握手头里的 early data。
 * 客户端形如：
 *   Sec-WebSocket-Protocol: <base64url(首包)>, <其它协议>
 *   Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits
 * ed 参数（?ed=2048）告诉客户端「首包可以提前塞进头里」。
 * 返回 Uint8Array 数组，解析失败返回 []。
 */
function b64urlDecode(str) {
	const s = String(str || '').trim().replace(/-/g, '+').replace(/_/g, '/');
	const pad = s.length % 4 === 0 ? '' : '='.repeat(4 - (s.length % 4));
	const bin = atob(s + pad);
	const out = new Uint8Array(bin.length);
	for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
	return out;
}

function readEarlyData(request) {
	const chunks = [];

	// 1) Sec-WebSocket-Protocol：主流实现（Shadowrocket / sing-box / v2rayN）
	const proto = request.headers.get('Sec-WebSocket-Protocol');
	if (proto) {
		for (const part of proto.split(',')) {
			const p = part.trim();
			if (!p) continue;
			// 跳过纯握手协议名（如 vless / h2 / http/1.1 这类短标识）
			if (/^[a-z0-9._-]{1,20}$/i.test(p)) continue;
			try {
				const bytes = b64urlDecode(p);
				if (bytes.byteLength) chunks.push(bytes);
			} catch (e) { /* 不是 base64 */ }
		}
	}

	// 2) 部分客户端用自定义头
	const extra = request.headers.get('Early-Data') || request.headers.get('Sec-WebSocket-Early-Data');
	if (extra) {
		try {
			const bytes = b64urlDecode(extra);
			if (bytes.byteLength) chunks.push(bytes);
		} catch (e) { /* 忽略 */ }
	}

	return chunks;
}

async function handleWs(request, env, url) {
	const cfg = readConfig(env);

	const pair = new WebSocketPair();
	const [client, server] = Object.values(pair);
	server.accept();

	// 读池走 core/pool.js（SSTP_POOL → KV → POOL_URL → ASSETS）。
	// 不要 fetch 自己的 origin：那会命中同一个 Worker 形成环，
	// Cloudflare 直接以 1042 拒绝。
	const pickNodes = () => loadPool(env, cfg);
	const pin = parseNodePin(url.searchParams.get('node'));

	const openUpstream = createUpstreamOpener({
		env, cfg, dial, resolve: resolveA,
		pickNodes,
		pinnedId: pin.pinnedId,
		pinnedIndex: pin.pinnedIndex,
		onLog: (m) => console.log('[ws]', m),
	});

	// 客户端把 VLESS 首包塞进握手的 Sec-WebSocket-Protocol 头（early data）
	const earlyData = readEarlyData(request);

	createVlessSession({
		uuid: cfg.UUID,
		openUpstream,
		ws: {
			onMessage: (cb) => server.addEventListener('message', ev => cb(ev.data)),
			onClose: (cb) => {
				server.addEventListener('close', cb);
				server.addEventListener('error', cb);
			},
			send: (u8) => { try { server.send(u8); } catch (e) { } },
			close: () => { try { server.close(); } catch (e) { } },
			drainEarlyData: () => earlyData,
		},
		onLog: (m) => console.log('[vless]', m),
	});

	// 回显 Sec-WebSocket-Protocol：客户端用它携带 early data，
	// 不回显则严格实现会判定子协议协商失败并断开。
	const headers = new Headers();
	const offeredProto = request.headers.get('Sec-WebSocket-Protocol');
	if (offeredProto) headers.set('Sec-WebSocket-Protocol', offeredProto);

	return new Response(null, { status: 101, webSocket: client, headers });
}
