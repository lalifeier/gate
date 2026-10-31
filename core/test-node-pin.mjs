// 验证订阅里 ?node=<nodeId> 是否真的固定到指定节点（以及旧格式 ?node=<下标> 的兼容）
//   DIAL_PROXY=http://127.0.0.1:1080 node core/test-node-pin.mjs 0 1 2
//   node core/test-node-pin.mjs --id <nodeId>

import http from 'node:http';
import fs from 'node:fs/promises';
import { WebSocket, WebSocketServer } from 'ws';

import { routeHttp, readConfig } from './router.js';
import { createNodeDial, resolveA } from './dial-node.js';
import { createVlessSession } from './vless-ws.js';
import { createUpstreamOpener, parseNodePin } from './upstream.js';
import { projectPool, nodeIdOf } from './schema.js';
import { uuidBytes } from './vless.js';

const UUID = process.env.UUID || 'b2f9a3c4-1d2e-4f5a-8b9c-0d1e2f3a4b5c';
const ADMIN = process.env.ADMIN || 'selftest';

const rawPool = JSON.parse(await fs.readFile(new URL('../public/nodes.json', import.meta.url), 'utf8'));
const pool = projectPool(rawPool);

const args = process.argv.slice(2);
const idFlag = args.indexOf('--id');
const pins = idFlag >= 0
	? args.slice(idFlag + 1, idFlag + 2)
	: args.filter(a => /^\d+$/.test(a));

const env = { UUID, ADMIN, CHAIN: 'always', __readFile: (n) => fs.readFile(new URL(`../public/${n}`, import.meta.url), 'utf8') };
const dial = createNodeDial({ insecure: true, connectTimeoutMs: 20000 });

const server = http.createServer(async (req, res) => {
	const url = new URL(req.url, `http://${req.headers.host}`);
	const r = await routeHttp({
		pathname: url.pathname, query: url.searchParams, origin: url.origin,
		hostname: url.hostname, env, dial, resolve: resolveA, cf: null,
	});
	res.writeHead(r.status, r.headers);
	res.end(r.body);
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
	const url = new URL(req.url, `http://${req.headers.host}`);
	wss.handleUpgrade(req, socket, head, (ws) => {
		const cfg = readConfig(env);
		const pin = parseNodePin(url.searchParams.get('node'));
		const openUpstream = createUpstreamOpener({
			cfg, dial, resolve: resolveA,
			pickNodes: async () => pool,
			pinnedId: pin.pinnedId,
			pinnedIndex: pin.pinnedIndex,
			onLog: (m) => console.log('    ' + m),
		});
		createVlessSession({
			uuid: cfg.UUID,
			openUpstream,
			ws: {
				onMessage: (cb) => ws.on('message', d => cb(new Uint8Array(d))),
				onClose: (cb) => ws.on('close', cb),
				send: (u8) => ws.send(u8, { binary: true }),
				close: () => ws.close(),
			},
		});
	});
});

await new Promise(r => server.listen(0, '127.0.0.1', r));
const port = server.address().port;

function probe(pinValue) {
	return new Promise((resolve) => {
		const ws = new WebSocket(`ws://127.0.0.1:${port}/?ed=2048&node=${encodeURIComponent(pinValue)}`);
		ws.on('open', () => {
			const host = Buffer.from('ip-api.com');
			const req = Buffer.from('GET /json/?fields=query HTTP/1.1\r\nHost: ip-api.com\r\nConnection: close\r\n\r\n');
			ws.send(Buffer.concat([
				Buffer.from([0]), uuidBytes(UUID), Buffer.from([0, 1, 0, 80, 2, host.length]), host, req,
			]), { binary: true });
		});
		let buf = Buffer.alloc(0);
		const timer = setTimeout(() => { ws.terminate(); resolve('TIMEOUT'); }, 40000);
		ws.on('message', (d) => {
			buf = Buffer.concat([buf, Buffer.from(d)]);
			if (buf.length > 80) {
				clearTimeout(timer);
				ws.close();
				const text = buf.subarray(2).toString('utf8');
				const m = text.match(/"query":"([\d.]+)"/);
				resolve(m ? m[1] : text.slice(0, 60));
			}
		});
		ws.on('error', (e) => { clearTimeout(timer); resolve('ERR ' + e.message); });
	});
}

const targets = pins.length ? pins : ['0', '1', '2'];
for (const pin of targets) {
	const isId = !/^\d+$/.test(pin);
	const node = isId
		? pool.find(n => n.nodeId === pin)
		: pool[Number(pin)];
	if (!node) {
		console.log(`node=${pin} 不存在（池中 ${pool.length} 个，按 ${isId ? 'nodeId' : '下标'} 查）`);
		continue;
	}
	const label = isId ? `nodeId=${pin}` : `node=${pin}（旧下标）`;
	console.log(`${label}  固定 -> ${node.host}:${node.port}  [${nodeIdOf(node.host, node.port)}]`);
	const exitIp = await probe(pin);
	console.log(`          出口 ${exitIp}`);
}

server.closeAllConnections?.();
server.close();
process.exit(0);
