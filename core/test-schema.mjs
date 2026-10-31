// 校验 core/schema.js 是字段的唯一来源：
// 1. 探测结果的字段必须能完整穿过 projectNode，不丢字段
// 2. pipeline 的 data.json 与 Worker 的 /data.json 必须同构
// 3. 页面模板读取的每个字段都必须存在
//
//   node core/test-schema.mjs

import { readFile } from 'node:fs/promises';
import { projectNode, projectPool, VIEW_FIELDS, POOL_FIELDS } from './schema.js';

let failed = 0;
const ok = (cond, label, extra = '') => {
	console.log(cond ? `  ✓ ${label}` : `  ✗ ${label}`, extra);
	if (!cond) failed++;
};

// 一份「完整」的探测结果：所有字段都填上真实值
const SAMPLE = {
	host: 'vpn123456.opengw.net',
	port: 1710,
	ip: '203.0.113.7',
	country: 'Japan',
	code: 'jp',
	tag: 'JP-住宅-03',
	latency_ms: 1763,
	resident: 'residential',
	exit: {
		ip: '126.122.153.76',
		country: 'Japan',
		country_code: 'JP',
		city: 'Minato',
		org: 'SoftBank Corp.',
		asn: { org: 'SoftBank Corp.', asn: 'AS17676 SoftBank Corp.' },
		type: 'residential',
	},
	assigned_ip: '10.211.1.3',
	colo: 'HKG',
	via: 'worker',
	checked_at: '2026-10-30T19:47:39.000Z',
	success: true,
	error: null,
};

console.log('[1] projectNode 不丢字段');
const poolNode = projectNode(SAMPLE);
for (const key of Object.keys(POOL_FIELDS)) {
	const src = key === 'code' ? 'jp' : SAMPLE[key];
	const expectEmpty = src === undefined || src === null || src === '';
	const got = poolNode[key];
	const present = got !== undefined && (expectEmpty ? true : got !== null);
	if (!present) { ok(false, `字段 ${key} 丢失`); continue; }
	// 关键字段做值校验
	if (key === 'exit') ok(got?.ip === SAMPLE.exit.ip, 'exit.ip 保留', got?.ip);
	else if (key === 'checked_at') ok(got === SAMPLE.checked_at, 'checked_at 保留', got);
	else if (key === 'code') ok(got === 'JP', 'code 大写化', got);
	else if (key === 'port') ok(got === 1710, 'port 数值化', got);
	else ok(true, `字段 ${key} = ${JSON.stringify(got)}`.slice(0, 60));
}

console.log('\n[2] 展示层字段（VIEW_FIELDS）齐全');
const viewNode = projectNode(SAMPLE, VIEW_FIELDS);
for (const key of ['host', 'port', 'country_code', 'latency_ms', 'resident', 'residential', 'exit', 'checked_at']) {
	ok(viewNode[key] !== undefined, `view.${key} 存在`, JSON.stringify(viewNode[key])?.slice(0, 40));
}
ok(viewNode.protocol === 'sstp', 'view.protocol 默认 sstp');
ok(viewNode.residential === 'residential', 'view.residential 是 resident 的别名');

console.log('\n[3] projectPool 接受多种输入形式');
ok(projectPool([SAMPLE]).length === 1, '数组');
ok(projectPool({ nodes: [SAMPLE] }).length === 1, '{nodes:[]}');
ok(projectPool(JSON.stringify([SAMPLE])).length === 1, 'JSON 字符串');
ok(projectPool([{ host: '' }]).length === 0, '无效 host 被过滤');

console.log('\n[4] 页面模板读取的字段都在 VIEW_FIELDS 里');
const html = await readFile(new URL('../web/index.html', import.meta.url), 'utf8');
// 抓 n.xxx / node.xxx 形式的字段访问
const accessed = new Set();
for (const m of html.matchAll(/\bn\.([a-z_][a-z0-9_]*)/gi)) accessed.add(m[1]);
for (const m of html.matchAll(/\bx\[1\]\.nodes|g\.nodes|g\.count|g\.residential|g\.datacenter|g\.code/gi)) {
	for (const f of ['nodes', 'count', 'residential', 'datacenter', 'code']) accessed.add(f);
}
const groupFields = new Set(['nodes', 'count', 'residential', 'datacenter', 'code']);
const statsFields = new Set(['sstp_nodes', 'checked', 'success', 'countries', 'residential_est', 'datacenter_est']);
const missing = [...accessed].filter(f => !(f in VIEW_FIELDS) && !groupFields.has(f) && !statsFields.has(f));
ok(missing.length === 0, `模板访问的字段全部有定义`, missing.length ? `缺少: ${missing.join(', ')}` : `${accessed.size} 个`);

console.log('\n[5] pipeline 与 Worker 的 data.json 同构');
const rawData = JSON.parse(await readFile(new URL('../public/data.json', import.meta.url), 'utf8'));
const firstGroup = Object.values(rawData.countries || {})[0];
const firstNode = firstGroup?.nodes?.[0];
if (!firstNode) {
	console.log('  - public/data.json 还没有节点，跳过');
} else {
	const required = ['host', 'port', 'country_code', 'latency_ms', 'resident', 'residential', 'exit', 'checked_at', 'protocol', 'link'];
	const absent = required.filter(f => !(f in firstNode));
	ok(absent.length === 0, 'data.json 节点字段齐全', absent.length ? `缺少: ${absent.join(', ')}` : required.length + ' 个字段');
	ok(!!(rawData.stats && 'sstp_nodes' in rawData.stats), 'stats.sstp_nodes 存在');
	ok(!!firstNode.exit, '节点带 exit（出口信息）', firstNode.exit?.org || '');
	ok(!!firstNode.checked_at, '节点带 checked_at（检测时间）', firstNode.checked_at || '');
}

console.log(failed ? `\n失败 ${failed} 项` : '\n全部通过');
process.exit(failed ? 1 : 0);
