// 把检测产物转成 Worker / Vercel 可直接读的节点池
//   public/data.json  (详情页用)
//   public/nodes.json (节点池，Worker 侧 /api/nodes 与 KV 写入用)
//   public/nodes.txt  (订阅行格式：入口#标签$sstp://user:pass@host:port)
//
// 由 pipeline/check.mjs 直接产出，本脚本用于二次转换已有 data.json。

import { readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const PUBLIC_DIR = process.env.PUBLIC_DIR || join(ROOT, 'public');

const data = JSON.parse(await readFile(join(PUBLIC_DIR, 'data.json'), 'utf8'));

const all = Object.values(data.countries || {}).flat();
const pool = all
	.map(n => ({ host: n.host, port: n.port, country: n.country, code: n.code, tag: n.host, latency_ms: n.latency_ms ?? null }))
	.sort((a, b) => (a.latency_ms ?? 1e9) - (b.latency_ms ?? 1e9));

await writeFile(join(PUBLIC_DIR, 'nodes.json'), JSON.stringify(pool, null, 1));
console.log(`nodes.json: ${pool.length} 个节点`);
