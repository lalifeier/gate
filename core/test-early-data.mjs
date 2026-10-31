// 验证 early data 路径：模拟 Shadowrocket 把 VLESS 首包塞进
// Sec-WebSocket-Protocol 头，确认服务端能正确解析并完成隧道。
//
//   node core/test-early-data.mjs              # 本地 direct
//   DIAL_PROXY=... node core/test-early-data.mjs --sstp

import http from 'node:http';
import { WebSocket } from 'ws';
import { WebSocketServer } from 'ws';

import { createNodeDial, resolveA } from './dial-node.js';
import { routeHttp, readConfig, parsePool } from './router.js';
import { createVlessSession } from './vless-ws.js';
import { createUpstreamOpener, parseNodePin } from './upstream.js';
import { uuidBytes } from './vless.js';

const USE_SSTP = process.argv.includes('--sstp');
const UUID = 'b2f9a3c4-1d2e-4f5a-8b9c-0d1e2f3a4b5c';

const ENV = {
	UUID,
	ADMIN: 'edtest',
	CHAIN: USE_SSTP ? 'always' : 'direct',
	SSTP_POOL: process.env.SSTP_POOL || 'public-vpn-127.opengw.net:443',
	SSTP_USER: 'vpn',
	SSTP_PASS: 'vpn',
	SSTP_TIMEOUT_MS: '30000',
};

function b64urlDecode(str) {
	const t = String(str || '').trim().replace(/-/g, '+').replace(/_/g, '/');
	const pad = t.length % 4 === 0 ? '' : '='.repeat(4 - (t.length % 4));
	return new Uint8Array(Buffer.from(t + pad, 'base64'));
}

// 与 Worker / Vercel 适配层一致的解析逻辑
function readEarlyData(headers) {
	const chunks = [];
	const proto = headers['sec-websocket-protocol'];
	if (proto) {
		for (const part of String(proto).split(',')) {
			const p = part.trim();
			if (!p) continue;
			if (/^[a-z0-9._-]{1,20}$/i.test(p)) continue;
			try {
				const b = b64urlDecode(p);
				if (b.byteLength) chunks.push(b);
			} catch (e) { }
		}
	}
	const extra = headers['early-data'] || headers['sec-websocket-early-data'];
	if (extra) {
		try {
			const b = b64urlDecode(extra);
			if (b.byteLength) chunks.push(b);
		} catch (e) { }
	}
	return chunks;
}

const server = http.createServer(async (req, res) => {
	const url = new URL(req.url, `http://${req.headers.host}`);
	const r = await routeHttp({
		pathname: url.pathname, query: url.searchParams, origin: url.origin,
		hostname: url.hostname, env: ENV,
		dial: createNodeDial({ insecure: true, proxyUrl: process.env.DIAL_PROXY || '' }),
		resolve: resolveA, cf: null,
	});
	res.writeHead(r.status, r.headers);
	res.end(r.body);
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
	wss.handleUpgrade(req, socket, head, (ws) => {
		const url = new URL(req.url, `http://${req.headers.host}`);
		const cfg = readConfig(ENV);
		const pin = parseNodePin(url.searchParams.get('node'));
		const openUpstream = createUpstreamOpener({
			cfg,
			dial: createNodeDial({ insecure: true, proxyUrl: process.env.DIAL_PROXY || '' }),
			resolve: resolveA,
			pickNodes: async () => parsePool(cfg),
			pinnedId: pin.pinnedId,
			pinnedIndex: pin.pinnedIndex,
			onLog: (m) => console.log('    [upstream]', m),
		});
		createVlessSession({
			uuid: cfg.UUID,
			openUpstream,
			ws: {
				onMessage: (cb) => ws.on('message', (d, bin) => cb(bin ? new Uint8Array(d) : new Uint8Array(Buffer.from(d)))),
				onClose: (cb) => { ws.on('close', cb); ws.on('error', cb); },
				send: (u8) => { try { ws.send(u8, { binary: true }); } catch (e) { } },
				close: () => { try { ws.close(); } catch (e) { } },
				drainEarlyData: () => readEarlyData(req.headers),
			},
			onLog: (m) => console.log('    [vless]', m),
		});
	});
});

await new Promise(r => server.listen(0, '127.0.0.1', r));
const port = server.address().port;

// 构造 VLESS 首包（含 HTTP 请求）
function buildFirstPacket(host, target) {
	const addr = Buffer.from(host, 'utf8');
	const payload = Buffer.from(
		`GET ${target} HTTP/1.1\r\nHost: ${host}\r\nUser-Agent: ed-test\r\nConnection: close\r\n\r\n`,
	);
	return Buffer.concat([
		Buffer.from([0]),
		uuidBytes(UUID),
		Buffer.from([0]),
		Buffer.from([1]),
		Buffer.from([0, 80]),
		Buffer.from([2, addr.length]),
		addr,
		payload,
	]);
}

// 用 Sec-WebSocket-Protocol 携带 early data，模拟 Shadowrocket
function fakeShadowrocket(firstPacket) {
	return new Promise((resolve) => {
		const proto = firstPacket.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
		const ws = new WebSocket(`ws://127.0.0.1:${port}/?ed=2048`, [proto]);
		const chunks = [];
		const timer = setTimeout(() => { ws.terminate(); resolve({ ok: false, detail: 'timeout' }); }, 45000);

		ws.on('message', (d) => {
			const b = Buffer.from(d);
			chunks.push(b);
			const total = chunks.reduce((n, x) => n + x.length, 0);
			if (total > 60) {
				clearTimeout(timer);
				ws.close();
				const all = Buffer.concat(chunks);
				const text = all.subarray(2).toString('utf8');
				resolve({ ok: /HTTP\/1\.[01] 2\d\d/.test(text), detail: text.split('\r\n')[0] || '(空)' });
			}
		});
		ws.on('error', (e) => { clearTimeout(timer); resolve({ ok: false, detail: e.message }); });
		ws.on('close', () => {
			clearTimeout(timer);
			const all = Buffer.concat(chunks);
			if (all.length) {
				const text = all.subarray(2).toString('utf8');
				resolve({ ok: /HTTP\/1\.[01] 2\d\d/.test(text), detail: text.split('\r\n')[0] || '(空)' });
			} else {
				resolve({ ok: false, detail: '连接被关闭且无数据' });
			}
		});
	});
}

console.log(`本地服务 127.0.0.1:${port}  CHAIN=${ENV.CHAIN}\n`);

const host = USE_SSTP ? 'ip-api.com' : 'example.com';
const target = USE_SSTP ? '/json/?fields=query' : '/';

console.log('[1] early data 走 Sec-WebSocket-Protocol');
const pkt = buildFirstPacket(host, target);
console.log(`    首包 ${pkt.length} 字节`);
const r = await fakeShadowrocket(pkt);
console.log(`    ${r.ok ? '✓' : '✗'} ${r.detail}`);

console.log('\n[2] 对照：不带 early data，首包走普通 message');
const r2 = await new Promise((resolve) => {
	const ws = new WebSocket(`ws://127.0.0.1:${port}/?ed=2048`);
	const chunks = [];
	const timer = setTimeout(() => { ws.terminate(); resolve({ ok: false, detail: 'timeout' }); }, 45000);
	ws.on('open', () => ws.send(buildFirstPacket(host, target), { binary: true }));
	ws.on('message', (d) => {
		chunks.push(Buffer.from(d));
		if (chunks.reduce((n, x) => n + x.length, 0) > 60) {
			clearTimeout(timer); ws.close();
			const text = Buffer.concat(chunks).subarray(2).toString('utf8');
			resolve({ ok: /HTTP\/1\.[01] 2\d\d/.test(text), detail: text.split('\r\n')[0] });
		}
	});
	ws.on('error', (e) => { clearTimeout(timer); resolve({ ok: false, detail: e.message }); });
});
console.log(`    ${r2.ok ? '✓' : '✗'} ${r2.detail}`);

server.closeAllConnections?.();
server.close();
process.exit(r.ok && r2.ok ? 0 : 1);
