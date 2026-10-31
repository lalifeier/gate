// Cloudflare Worker 手工部署脚本（无需 wrangler/workerd 二进制）
// 用 Cloudflare REST API 直接把打包好的 worker 上传。
//
//   CF_API_TOKEN=... CF_ACCOUNT_ID=... CF_KV_ID=... node scripts/deploy-worker.mjs
//
// 需要的 token 权限：Workers Scripts:Edit, Workers KV Storage:Edit

import { execFileSync } from 'node:child_process';
import { readFile, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const TOKEN = process.env.CF_API_TOKEN;
const ACCOUNT = process.env.CF_ACCOUNT_ID;
const SCRIPT = process.env.CF_SCRIPT_NAME || 'vpngate-edge';
const KV_ID = process.env.CF_KV_ID || '';

if (!TOKEN || !ACCOUNT) {
	console.error('需要 CF_API_TOKEN 与 CF_ACCOUNT_ID');
	process.exit(1);
}

const dir = await mkdtemp(join(tmpdir(), 'vpngate-'));
const out = join(dir, 'worker.js');

execFileSync('node_modules/.bin/esbuild', [
	'worker/src/index.js',
	'--bundle',
	'--format=esm',
	'--outfile', out,
	'--external:cloudflare:sockets',
], { stdio: 'inherit' });

const bundle = await readFile(out, 'utf8');

const metadata = {
	main_module: 'worker.js',
	compatibility_date: '2025-11-04',
	compatibility_flags: ['nodejs_compat'],
	bindings: [
		{ type: 'plain_text', name: 'UUID', text: process.env.UUID || '' },
		{ type: 'plain_text', name: 'ADMIN', text: process.env.ADMIN || '' },
		{ type: 'plain_text', name: 'CHAIN', text: process.env.CHAIN || 'always' },
		{ type: 'plain_text', name: 'SSTP_POOL', text: process.env.SSTP_POOL || '' },
		{ type: 'plain_text', name: 'SSTP_USER', text: process.env.SSTP_USER || 'vpn' },
		{ type: 'plain_text', name: 'SSTP_PASS', text: process.env.SSTP_PASS || 'vpn' },
		{ type: 'plain_text', name: 'PROBE_TARGET', text: process.env.PROBE_TARGET || 'ip-api.com' },
		{ type: 'plain_text', name: 'POOL_URL', text: process.env.POOL_URL || '' },
		{ type: 'plain_text', name: 'POOL_CACHE_TTL_MS', text: process.env.POOL_CACHE_TTL_MS || '90000' },
		{ type: 'plain_text', name: 'EDGE_HOSTS', text: process.env.EDGE_HOSTS || '' },
		{ type: 'plain_text', name: 'NODE_HOST', text: process.env.NODE_HOST || '' },
		...(KV_ID ? [{ type: 'kv_namespace', name: 'KV', namespace_id: KV_ID }] : []),
	],
};

const form = new FormData();
form.append('metadata', new Blob([JSON.stringify(metadata)], { type: 'application/json' }));
form.append('worker.js', new Blob([bundle], { type: 'application/javascript+module' }), 'worker.js');

const res = await fetch(
	`https://api.cloudflare.com/client/v4/accounts/${ACCOUNT}/workers/scripts/${SCRIPT}`,
	{ method: 'PUT', headers: { authorization: `Bearer ${TOKEN}` }, body: form },
);
const json = await res.json();
if (!json.success) {
	console.error(JSON.stringify(json.errors || json, null, 2));
	process.exit(1);
}
console.log(`已上传 ${SCRIPT}（${(bundle.length / 1024).toFixed(1)} KB）`);
