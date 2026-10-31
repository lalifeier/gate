// 对已部署的 Worker/Vercel 做真实 WebSocket + VLESS 连通性测试
//   node core/test-remote.mjs https://edt.example.com
//   HTTPS_PROXY=http://127.0.0.1:1080 node core/test-remote.mjs https://域名

import { WebSocket } from 'ws';
import { Agent, setGlobalDispatcher } from 'undici';
import { uuidBytes } from './vless.js';

const BASE = (process.argv[2] || 'https://edt.example.com').replace(/\/$/, '');
const UUID = process.env.UUID || 'b2f9a3c4-1d2e-4f5a-8b9c-0d1e2f3a4b5c';
const PATHS = process.env.TEST_PATHS ? process.env.TEST_PATHS.split(',') : ['/?ed=2048', '/', '/ws'];

if (process.env.HTTPS_PROXY) {
	const proxyUrl = new URL(process.env.HTTPS_PROXY);
	setGlobalDispatcher(new Agent({
		connect: { uri: `http://${proxyUrl.hostname}:${proxyUrl.port}` },
	}));
}

function vlessRequest(host, port, payload) {
	const addr = Buffer.from(host, 'utf8');
	return Buffer.concat([
		Buffer.from([0]),
		uuidBytes(UUID),
		Buffer.from([0]),
		Buffer.from([1]),
		Buffer.from([(port >> 8) & 0xff, port & 0xff]),
		Buffer.from([2, addr.length]),
		addr,
		payload,
	]);
}

function testWs(wsUrl) {
	return new Promise((resolve) => {
		const t0 = Date.now();
		let opened = false;
		const ws = new WebSocket(wsUrl, { handshakeTimeout: 20000 });
		const timer = setTimeout(() => {
			ws.terminate();
			resolve({ ok: false, stage: opened ? 'no-data' : 'handshake', ms: Date.now() - t0, detail: 'timeout' });
		}, 45000);

		ws.on('open', () => {
			opened = true;
			const q = Buffer.from('GET /json/?fields=query HTTP/1.1\r\nHost: ip-api.com\r\nConnection: close\r\n\r\n');
			ws.send(vlessRequest('ip-api.com', 80, q), { binary: true });
		});
		ws.on('unexpected-response', (req, res) => {
			clearTimeout(timer);
			ws.terminate();
			resolve({ ok: false, stage: 'http', ms: Date.now() - t0, detail: `HTTP ${res.statusCode} ${res.headers.location || ''}`.trim() });
		});
		ws.on('message', (d) => {
			const buf = Buffer.from(d);
			if (buf.length > 40) {
				clearTimeout(timer);
				ws.close();
				const text = buf.subarray(2).toString('utf8');
				const m = text.match(/"query":"([\d.]+)"/);
				resolve({ ok: !!m, stage: 'data', ms: Date.now() - t0, detail: m ? `出口 ${m[1]}` : text.slice(0, 60) });
			}
		});
		ws.on('error', (e) => {
			clearTimeout(timer);
			resolve({ ok: false, stage: opened ? 'data' : 'handshake', ms: Date.now() - t0, detail: e.message });
		});
	});
}

console.log(`目标 ${BASE}`);
console.log(`UUID ${UUID}`);
console.log('');

let failed = 0;
for (const p of PATHS) {
	const wsUrl = `${BASE.replace(/^http/, 'ws')}${p}`;
	process.stdout.write(`  ${p.padEnd(14)} `);
	const r = await testWs(wsUrl);
	if (!r.ok) failed++;
	console.log(`${r.ok ? '✓' : '✗'} ${r.stage} ${r.ms}ms  ${r.detail}`);
}

// 对照：普通 HTTP 路由是否正常
console.log('');
for (const p of ['/healthz', '/api/nodes', '/sub?token=' + (process.env.ADMIN || 'selftest') + '&format=text']) {
	try {
		const res = await fetch(BASE + p, { signal: AbortSignal.timeout(20000) });
		const body = await res.text();
		console.log(`  HTTP ${p.padEnd(30)} ${res.status}  ${body.replace(/\s+/g, ' ').slice(0, 50)}`);
	} catch (e) {
		console.log(`  HTTP ${p.padEnd(30)} ERR  ${e.message}`);
		failed++;
	}
}

console.log(failed ? `\n失败 ${failed} 项` : '\n全部通过');
process.exit(failed ? 1 : 0);
