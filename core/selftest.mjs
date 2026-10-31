// 端到端自测：起本地 Vercel 同款 server（api/index.js 逻辑），
// 用 VLESS 客户端连上去，验证 HTTP 路由 + VLESS-over-WS 隧道。
//
//   node core/selftest.mjs            # 默认 CHAIN=direct，验证 WS 转发
//   DIAL_PROXY=http://127.0.0.1:1080 node core/selftest.mjs --sstp
//        # CHAIN=always + 真实 SSTP 节点（需要能访问外网或代理）

import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { WebSocket } from 'ws';
import { WebSocketServer } from 'ws';
import crypto from 'node:crypto';

import { createNodeDial, resolveA } from './dial-node.js';
import { routeHttp, readConfig, parsePool, fallbackIndex } from './router.js';
import { loadPool } from './pool.js';
import { createVlessSession } from './vless-ws.js';
import { createUpstreamOpener, parseNodePin } from './upstream.js';
import { uuidBytes } from './vless.js';

const USE_SSTP = process.argv.includes('--sstp');
const UUID = 'b2f9a3c4-1d2e-4f5a-8b9c-0d1e2f3a4b5c';

const ENV = {
	UUID,
	ADMIN: 'selftest',
	CHAIN: USE_SSTP ? 'always' : 'direct',
	SSTP_POOL: process.env.SSTP_POOL || 'public-vpn-127.opengw.net:443',
	SSTP_USER: 'vpn',
	SSTP_PASS: 'vpn',
	PROBE_TIMEOUT_MS: '30000',
	SSTP_TIMEOUT_MS: '30000',
	// 让 core/pool.js 能回落到仓库里的 public/nodes.json
	__readFile: (name) => readFile(new URL(`../public/${name}`, import.meta.url), 'utf8'),
};

function makeServer() {
	const server = http.createServer();
	const wss = new WebSocketServer({ noServer: true });

	server.on('request', async (req, res) => {
		const url = new URL(req.url, `http://${req.headers.host}`);
		try {
			const r = await routeHttp({
				pathname: url.pathname, query: url.searchParams, origin: url.origin,
				hostname: url.hostname, env: ENV,
				dial: createNodeDial({ insecure: true, proxyUrl: process.env.DIAL_PROXY || '' }),
				resolve: resolveA, cf: { colo: 'LOCAL' },
			});

			if (r.asset) {
				// 模拟 Worker assets：优先 public/ 下的真实文件，缺失则内置首页兜底
				const rel = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
				try {
					const body = await readFile(new URL(`../public/${rel}`, import.meta.url));
					res.writeHead(200, { 'content-type': rel.endsWith('.json') ? 'application/json' : 'text/html; charset=utf-8' });
					res.end(body);
					return;
				} catch (e) {
					if (url.pathname === '/' || url.pathname === '/index.html') {
						const fb = await fallbackIndex({ origin: url.origin, env: ENV });
						res.writeHead(fb.status, fb.headers); res.end(fb.body);
						return;
					}
				}
			}

			res.writeHead(r.status, r.headers);
			res.end(r.body);
		} catch (e) {
			res.writeHead(500); res.end(String(e));
		}
	});

	server.on('upgrade', (req, socket, head) => {
		wss.handleUpgrade(req, socket, head, (ws) => {
			const url = new URL(req.url, `http://${req.headers.host}`);
			const cfg = readConfig(ENV);
			const dial = createNodeDial({ insecure: true, proxyUrl: process.env.DIAL_PROXY || '' });
			const pin = parseNodePin(url.searchParams.get('node'));
			const openUpstream = createUpstreamOpener({
				env: ENV, cfg, dial, resolve: resolveA,
				pickNodes: async () => loadPoolForTest(cfg),
				pinnedId: pin.pinnedId,
				pinnedIndex: pin.pinnedIndex,
				onLog: (m) => console.log('  [upstream]', m),
			});
			createVlessSession({
				uuid: cfg.UUID, openUpstream,
				ws: {
					onMessage: (cb) => ws.on('message', (d, bin) => cb(bin ? new Uint8Array(d) : new Uint8Array(Buffer.from(d)))),
					onClose: (cb) => { ws.on('close', cb); ws.on('error', cb); },
					send: (u8) => { try { ws.send(u8, { binary: true }); } catch (e) { } },
					close: () => { try { ws.close(); } catch (e) { } },
				},
				onLog: (m) => console.log('  [vless]', m),
			});
		});
	});

	return server;
}

// 测试用节点池：SSTP_POOL 可逗号分隔多个；同时支持从 public/nodes.json 读取
async function loadPoolForTest(cfg) {
	return await loadPool(ENV, cfg);
}

function buildVlessRequest(host, port, payload) {
	const addr = Buffer.from(host, 'utf8');
	const head = Buffer.concat([
		Buffer.from([0]),                       // ver
		uuidBytes(UUID),                        // uuid
		Buffer.from([0]),                       // addon len
		Buffer.from([1]),                       // cmd = TCP
		Buffer.from([(port >> 8) & 0xff, port & 0xff]),
		Buffer.from([2, addr.length]),          // atyp = domain
		addr,
		payload,
	]);
	return head;
}

// 简易 HTTP over VLESS：发 GET 到 host:port，读出响应
function vlessHttpGet(port, host, requestTarget) {
	return new Promise((resolve, reject) => {
		const ws = new WebSocket(`ws://127.0.0.1:${port}/?ed=2048`);
		const chunks = [];
		let got = 0;
		const timer = setTimeout(() => { ws.terminate(); reject(new Error('timeout')); }, 60000);

		ws.on('open', () => {
			const req = Buffer.from(
				`GET ${requestTarget} HTTP/1.1\r\nHost: ${host}\r\nUser-Agent: selftest\r\nConnection: close\r\n\r\n`,
			);
			ws.send(buildVlessRequest(host, 80, req), { binary: true });
		});
		ws.on('message', (data) => {
			const b = Buffer.from(data);
			chunks.push(b);
			got += b.length;
			if (got > 256) { clearTimeout(timer); ws.close(); resolve(Buffer.concat(chunks)); }
		});
		ws.on('error', (e) => { clearTimeout(timer); reject(e); });
		ws.on('close', () => { clearTimeout(timer); resolve(Buffer.concat(chunks)); });
	});
}

async function main() {
	const server = makeServer();
	await new Promise(r => server.listen(0, '127.0.0.1', r));
	const port = server.address().port;
	console.log(`本地 server 127.0.0.1:${port}  CHAIN=${ENV.CHAIN}`);

	let failed = 0;
	const ok = (cond, label, extra = '') => {
		console.log(cond ? `  ✓ ${label}` : `  ✗ ${label}`, extra);
		if (!cond) failed++;
	};

	// 1) HTTP 路由
	console.log('\n[1] HTTP 路由');
	for (const p of ['/healthz', '/api/nodes', '/nodes.txt', '/', '/admin?token=selftest', '/sub?token=selftest']) {
		const r = await fetch(`http://127.0.0.1:${port}${p}`);
		const body = await r.text();
		ok(r.ok, `GET ${p} -> ${r.status}`, body.replace(/\s+/g, ' ').slice(0, 70));
	}
	{
		const r = await fetch(`http://127.0.0.1:${port}/admin?token=wrong`);
		ok(r.status === 403, `/admin 错 token -> ${r.status}`);
	}

	// 2) VLESS over WS -> 上游 TCP
	console.log('\n[2] VLESS over WS');
	const target = USE_SSTP ? ['ip-api.com', '/json/?fields=status,query'] : ['example.com', '/'];
	try {
		const raw = await vlessHttpGet(port, target[0], target[1]);
		const s = raw.toString('latin1');
		// 前两字节是 VLESS 响应头 0x00 0x00
		ok(raw[0] === 0 && raw[1] === 0, 'VLESS 响应头 00 00');
		const text = raw.subarray(2).toString('utf8');
		ok(/HTTP\/1\.1 200/.test(text) || /HTTP\/1\.[01] 2\d\d/.test(text), '上游返回 HTTP 200', text.split('\r\n')[0]);
		console.log('     响应片段:', text.replace(/\s+/g, ' ').slice(0, 120));
	} catch (e) {
		ok(false, 'VLESS 隧道', e.message);
	}

	// 3) /check（仅 direct 模式跑，避免耗时；SSTP 模式仍要跑）
	console.log('\n[3] /check');
	if (USE_SSTP) {
		// VPN Gate 节点随时会掉，逐个试，任一成功即算通过
		const pool = await loadPoolForTest(readConfig(ENV));
		let passed = false, lastErr = '';
		for (const node of pool.slice(0, 3)) {
			const spec = encodeURIComponent(`vpn:vpn@${node.host}:${node.port}`);
			const t0 = Date.now();
			try {
				const r = await fetch(`http://127.0.0.1:${port}/check?sstp=${spec}`, { signal: AbortSignal.timeout(60000) });
				const j = await r.json();
				if (j.success) {
					ok(true, `/check ${node.host}:${node.port} ${Date.now() - t0}ms`,
						JSON.stringify({ exit: j.exit?.ip, org: j.exit?.org, type: j.exit?.type }));
					passed = true;
					break;
				}
				lastErr = `${node.host}:${node.port} -> ${j.error}`;
				console.log(`  - ${lastErr}，换下一个`);
			} catch (e) {
				lastErr = `${node.host}:${node.port} -> ${e.message}`;
				console.log(`  - ${lastErr}，换下一个`);
			}
		}
		if (!passed) ok(false, '/check 全部失败', lastErr);
	} else {
		console.log('  (direct 模式跳过，用 --sstp 测试真实节点)');
	}

	server.closeAllConnections?.();
	server.close();
	console.log(failed ? `\n失败 ${failed} 项` : '\n全部通过');
	process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
