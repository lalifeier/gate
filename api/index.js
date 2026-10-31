// Vercel Function（Node.js runtime）——与 Worker 同源的逻辑。
// 用 ws 包把 Node http server 的 upgrade 事件接进 core/vless-ws.js。

import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { createNodeDial, resolveA } from '../core/dial-node.js';
import { routeHttp, readConfig } from '../core/router.js';
import { loadPool } from '../core/pool.js';
import { createVlessSession } from '../core/vless-ws.js';
import { createUpstreamOpener, parseNodePin } from '../core/upstream.js';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const PUBLIC_DIR = process.env.PUBLIC_DIR || path.join(ROOT, 'public');

const server = http.createServer();
const wss = new WebSocketServer({ noServer: true });
const dial = createNodeDial({ insecure: true });

// 节点池走 core/pool.js：
//   SSTP_POOL → KV（Vercel 无）→ POOL_URL → public/nodes.json
// POOL_URL 指向定时任务发布的 GitHub Pages 快照。Vercel 没有 KV，
// 这是拿到每轮检测结果的途径；缺省时池会冻结在上次部署。
const env = {
	...process.env,
	__readFile: (name) => fs.readFile(path.join(PUBLIC_DIR, name), 'utf8'),
};

function b64urlDecode(str) {
	const t = String(str || '').trim().replace(/-/g, '+').replace(/_/g, '/');
	const pad = t.length % 4 === 0 ? '' : '='.repeat(4 - (t.length % 4));
	return new Uint8Array(Buffer.from(t + pad, 'base64'));
}

// 与 Worker 侧同逻辑：从握手头里取 early data
function readEarlyData(req) {
	const chunks = [];
	const proto = req.headers['sec-websocket-protocol'];
	if (proto) {
		for (const part of String(proto).split(',')) {
			const p = part.trim();
			if (!p) continue;
			if (/^[a-z0-9._-]{1,20}$/i.test(p)) continue;
			try {
				const b = b64urlDecode(p);
				if (b.byteLength) chunks.push(b);
			} catch (e) { /* 忽略 */ }
		}
	}
	const extra = req.headers['early-data'] || req.headers['sec-websocket-early-data'];
	if (extra) {
		try {
			const b = b64urlDecode(extra);
			if (b.byteLength) chunks.push(b);
		} catch (e) { /* 忽略 */ }
	}
	return chunks;
}

server.on('request', async (req, res) => {
	const url = new URL(req.url, `https://${req.headers.host || 'localhost'}`);

	try {
		const result = await routeHttp({
			pathname: url.pathname,
			query: url.searchParams,
			origin: url.origin,
			hostname: url.hostname,
			env,
			dial,
			resolve: resolveA,
			cf: null,
		});

		res.writeHead(result.status, result.headers);
		res.end(result.body);
	} catch (e) {
		res.writeHead(500, { 'content-type': 'application/json' });
		res.end(JSON.stringify({ error: e?.message || String(e) }));
	}
});

server.on('upgrade', (req, socket, head) => {
	wss.handleUpgrade(req, socket, head, (ws) => {
		const url = new URL(req.url, `https://${req.headers.host || 'localhost'}`);
		const cfg = readConfig(env);
		const pin = parseNodePin(url.searchParams.get('node'));

		const openUpstream = createUpstreamOpener({
			env, cfg, dial,
			resolve: resolveA,
			pickNodes: () => loadPool(env, cfg),
			pinnedId: pin.pinnedId,
			pinnedIndex: pin.pinnedIndex,
			onLog: (m) => console.log('[upstream]', m),
		});

		const earlyData = readEarlyData(req);

		createVlessSession({
			uuid: cfg.UUID,
			openUpstream,
			ws: {
				onMessage: (cb) => ws.on('message', (data, isBinary) => cb(isBinary ? new Uint8Array(data) : new Uint8Array(Buffer.from(data)))),
				onClose: (cb) => { ws.on('close', cb); ws.on('error', cb); },
				send: (u8) => { try { ws.send(u8, { binary: true }); } catch (e) { } },
				close: () => { try { ws.close(); } catch (e) { } },
				drainEarlyData: () => earlyData,
			},
			onLog: (m) => console.log('[vless]', m),
		});
	});
});

export default server;
