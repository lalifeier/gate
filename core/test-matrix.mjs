// 批量验证「订阅里每条链接」在真实部署环境下的可用性
//   HTTPS_PROXY=http://127.0.0.1:1080 node core/test-matrix.mjs https://域名
//   --limit 8       只测前 8 条
//   --node 3,7      只测指定下标

import { WebSocket } from 'ws';
import { Agent, setGlobalDispatcher } from 'undici';
import { uuidBytes } from './vless.js';

const BASE = (process.argv[2] || 'https://edt.example.com').replace(/\/$/, '');
const UUID = process.env.UUID || 'b2f9a3c4-1d2e-4f5a-8b9c-0d1e2f3a4b5c';
const ADMIN = process.env.ADMIN || 'selftest';

const argOf = (n, d) => { const i = process.argv.indexOf('--' + n); return i >= 0 ? process.argv[i + 1] : d; };
const only = argOf('node', '');
const limit = Number(argOf('limit', '0'));

if (process.env.HTTPS_PROXY) {
	const p = new URL(process.env.HTTPS_PROXY);
	setGlobalDispatcher(new Agent({ connect: { uri: `http://${p.hostname}:${p.port}` } }));
}

// 从订阅里取链接
const sub = await (await fetch(`${BASE}/sub?token=${ADMIN}&format=text`, { signal: AbortSignal.timeout(20000) })).text();
let links = sub.split('\n').filter(l => l.startsWith('vless://'));
if (only) {
	const keep = new Set(only.split(',').map(Number));
	links = links.filter((_, i) => keep.has(i));
}
if (limit > 0) links = links.slice(0, limit);

function vlessReq(host, port, payload) {
	const a = Buffer.from(host, 'utf8');
	return Buffer.concat([
		Buffer.from([0]), uuidBytes(UUID), Buffer.from([0]), Buffer.from([1]),
		Buffer.from([(port >> 8) & 0xff, port & 0xff]),
		Buffer.from([2, a.length]), a, payload,
	]);
}

function probe(link) {
	return new Promise((resolve) => {
		const u = new URL(link.replace(/^vless:\/\//, 'http://'));
		const name = decodeURIComponent(u.hash.slice(1));
		const wsPath = u.searchParams.get('path') + u.search; // path 里已含 ?ed=..&node=..
		const wsUrl = `wss://${u.hostname}:${u.port || 443}${u.searchParams.get('path')}${u.search ? '&' + u.search.slice(1) : ''}`;

		const t0 = Date.now();
		let opened = false;
		const ws = new WebSocket(wsUrl, { handshakeTimeout: 20000, headers: { Host: u.searchParams.get('host') } });
		const timer = setTimeout(() => { ws.terminate(); resolve({ name, ok: false, ms: Date.now() - t0, detail: opened ? '无数据' : '握手超时' }); }, 40000);

		ws.on('open', () => {
			opened = true;
			ws.send(vlessReq('ip-api.com', 80,
				Buffer.from('GET /json/?fields=query HTTP/1.1\r\nHost: ip-api.com\r\nConnection: close\r\n\r\n')),
				{ binary: true });
		});
		ws.on('message', (d) => {
			const b = Buffer.from(d);
			if (b.length > 40) {
				clearTimeout(timer); ws.close();
				const t = b.subarray(2).toString('utf8');
				const m = t.match(/"query":"([\d.]+)"/);
				resolve({ name, ok: !!m, ms: Date.now() - t0, detail: m ? '出口 ' + m[1] : t.slice(0, 50) });
			}
		});
		ws.on('unexpected-response', (_r, res) => {
			clearTimeout(timer); ws.terminate();
			resolve({ name, ok: false, ms: Date.now() - t0, detail: `HTTP ${res.statusCode} ${res.headers.location || ''}`.trim() });
		});
		ws.on('error', (e) => { clearTimeout(timer); resolve({ name, ok: false, ms: Date.now() - t0, detail: e.message }); });
	});
}

console.log(`共 ${links.length} 条待测，目标 ${BASE}\n`);
let ok = 0;
const results = [];
for (let i = 0; i < links.length; i++) {
	const r = await probe(links[i]);
	results.push(r);
	if (r.ok) ok++;
	console.log(`  ${String(i).padStart(2)} ${r.ok ? '✓' : '✗'} ${String(r.ms).padStart(5)}ms  ${r.name.padEnd(22)} ${r.detail}`);
}
console.log(`\n可用 ${ok}/${links.length}`);
process.exit(0);
