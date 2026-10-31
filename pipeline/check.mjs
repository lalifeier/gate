// 定时抓取 VPN Gate → 筛出 SSTP 节点 → 逐个探测可用性 → 产出 nodes.txt / data.json
// 用法：
//   node pipeline/check.mjs                 # 全量检测（默认最多 400 个）
//   node pipeline/check.mjs --limit 20      # 只测前 20 个，快速验证
//   node pipeline/check.mjs --no-probe      # 只抓取与筛选，不探测

import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { createNodeDial, resolveA } from '../core/dial-node.js';
import { sstpProbe } from '../core/sstp.js';
import { classifyExit } from '../core/classify.js';
import { projectNode, projectPool, sortByLatency, groupByCountry, buildStats, attachComputed, poolRev, poolAgeMinutes, VIEW_FIELDS } from '../core/schema.js';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

const argv = process.argv.slice(2);
const argOf = (name, dflt) => {
	const i = argv.indexOf(`--${name}`);
	return i >= 0 ? argv[i + 1] : dflt;
};
const hasFlag = (name) => argv.includes(`--${name}`);

const CONFIG = {
	VPNGATE_API: process.env.VPNGATE_API || 'http://www.vpngate.net/api/iphone/',
	VPNGATE_MIRROR: process.env.VPNGATE_MIRROR || 'https://raw.githubusercontent.com/fdciabdul/Vpngate-Scraper-API/main/json/data.json',
	// 云端检测端点，填到 /check?sstp= 为止，例如 https://edt.example.com/check?sstp=
	// 从云端测的延迟才反映「CF 边缘 → 客户端」这条真实路径。
	CHECK_WORKER: process.env.CHECK_WORKER || argOf('worker-url', ''),
	// 云端不可用时是否退回本地直连探测
	WORKER_FALLBACK: (process.env.WORKER_FALLBACK ?? 'true') !== 'false',
	CONCURRENCY: Number(process.env.CHECK_CONCURRENCY || argOf('concurrency', 24)),
	TIMEOUT_MS: Number(process.env.CHECK_TIMEOUT_MS || argOf('timeout', 15000)),
	LIMIT: Number(argOf('limit', process.env.MAX_CHECK_NODES || 400)),
	HTTP_TIMEOUT: Number(process.env.HTTP_TIMEOUT || 60000),
	PUBLIC_DIR: process.env.PUBLIC_DIR || join(ROOT, 'public'),
	NO_PROBE: hasFlag('no-probe'),
	KV_REST_URL: process.env.KV_REST_URL || '',   // 显式指定 KV 写地址（优先）
	CF_API_TOKEN: process.env.CF_API_TOKEN || '',
	CF_ACCOUNT_ID: process.env.CF_ACCOUNT_ID || '',
	CF_KV_ID: process.env.CF_KV_ID || '',
};

const dial = createNodeDial({ insecure: true, connectTimeoutMs: CONFIG.TIMEOUT_MS });


main().catch((e) => {
	console.error('[FATAL]', e);
	process.exit(1);
});

async function main() {
	const t0 = Date.now();
	const rows = await fetchVpngate();
	const sstp = extractSstpNodes(rows);
	const uniq = dedupe(sstp);
	log(`原始 ${rows.length} → SSTP ${sstp.length} → 去重 ${uniq.length}`);

	const slice = CONFIG.LIMIT > 0 ? uniq.slice(0, CONFIG.LIMIT) : uniq;

	let results;
	if (CONFIG.NO_PROBE) {
		results = slice.map(n => ({ ...n, success: true, latency_ms: null, resident: classify(n.host, ''), checked_at: now() }));
	} else {
		log('探测方式', CONFIG.CHECK_WORKER
			? `云端 ${CONFIG.CHECK_WORKER}${CONFIG.WORKER_FALLBACK ? '（失败退回本地）' : ''}`
			: '本地直连');
		results = await checkAll(slice);
	}

	const ok = results.filter(r => r.success);
	log(`检测 ${results.length}，可用 ${ok.length}，耗时 ${((Date.now() - t0) / 1000).toFixed(1)}s`);
	if (!CONFIG.NO_PROBE && results.length && !ok.length) {
		throw new Error('全部节点检测失败（数据源变化或网络不可达）');
	}

	await writeOutputs(results);
	await pushToKv(ok);
}

function log(...a) { console.log(`[${new Date().toISOString().slice(11, 19)}]`, ...a); }
function now() { return new Date().toISOString(); }

// ---------------------------------------------------------------------------
// 抓取
// ---------------------------------------------------------------------------
async function fetchVpngate() {
	const headers = { 'user-agent': 'Mozilla/5.0 (compatible; vpngate-edge/1.0)' };

	for (let attempt = 1; attempt <= 3; attempt++) {
		try {
			log(`拉取官方 API（第 ${attempt} 次）`, CONFIG.VPNGATE_API);
			const r = await fetch(CONFIG.VPNGATE_API, { headers, signal: AbortSignal.timeout(CONFIG.HTTP_TIMEOUT) });
			if (!r.ok) throw new Error(`HTTP ${r.status}`);
			const rows = parseCsv(await r.text());
			if (!rows.length) throw new Error('解析出 0 行');
			return rows;
		} catch (e) {
			log('官方 API 失败:', e.message);
			await sleep(Math.min(2000 * attempt, 6000));
		}
	}

	if (CONFIG.VPNGATE_MIRROR) {
		for (let attempt = 1; attempt <= 3; attempt++) {
			try {
				log(`回退镜像（第 ${attempt} 次）`, CONFIG.VPNGATE_MIRROR);
				const r = await fetch(CONFIG.VPNGATE_MIRROR, { headers, signal: AbortSignal.timeout(CONFIG.HTTP_TIMEOUT) });
				if (!r.ok) throw new Error(`HTTP ${r.status}`);
				const rows = parseMirror(await r.json());
				if (rows.length) return rows;
				throw new Error('镜像 0 行');
			} catch (e) {
				log('镜像失败:', e.message);
				await sleep(Math.min(2000 * attempt, 6000));
			}
		}
	}
	throw new Error('VPN Gate 官方源与镜像均不可用（已重试 3 次）');
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function parseCsv(text) {
	const lines = text.split(/\r?\n/).filter(l => l.trim());
	const headerIdx = lines.findIndex(l => l.replace(/^#+/, '').toLowerCase().startsWith('hostname'));
	if (headerIdx < 0) return [];

	const header = splitCsv(lines[headerIdx].replace(/^#+/, '')).map(h => h.trim().replace(/^\*/, '').toLowerCase());
	const col = (...names) => {
		for (const n of names) {
			const i = header.indexOf(n);
			if (i >= 0) return i;
		}
		return -1;
	};
	const iHost = col('hostname');
	const iIp = col('ip');
	const iLong = col('countrylong');
	const iShort = col('countryshort');
	const iB64 = col('openvpn_configdata_base64') >= 0 ? col('openvpn_configdata_base64') : header.findIndex(h => h.includes('base64'));

	const rows = [];
	for (let i = headerIdx + 1; i < lines.length; i++) {
		const f = splitCsv(lines[i]);
		if (f.length < 7) continue;
		const host = (f[iHost] || '').trim();
		const ip = (f[iIp] || '').trim();
		if (!host || !ip) continue;
		rows.push({
			host,
			ip,
			country_long: (f[iLong] || '').trim(),
			country_short: (f[iShort] || '').trim(),
			config_b64: iB64 >= 0 ? (f[iB64] || '').trim() : '',
		});
	}
	return rows;
}

function splitCsv(line) {
	const out = [];
	let cur = '', q = false;
	for (let i = 0; i < line.length; i++) {
		const c = line[i];
		if (q) {
			if (c === '"') {
				if (line[i + 1] === '"') { cur += '"'; i++; } else q = false;
			} else cur += c;
		} else if (c === '"') q = true;
		else if (c === ',') { out.push(cur); cur = ''; }
		else cur += c;
	}
	out.push(cur);
	return out;
}

function parseMirror(data) {
	const flat = [];
	const walk = (item) => {
		if (!item || typeof item !== 'object') return;
		if (Array.isArray(item.servers)) { flat.push(...item.servers); return; }
		flat.push(item);
	};
	(Array.isArray(data) ? data : [data]).forEach(walk);
	return flat.map(s => ({
		host: String(s.hostname || s.host || '').trim(),
		ip: String(s.ip || '').trim(),
		country_long: String(s.countrylong || s.country_long || s.country || '').trim(),
		country_short: String(s.countryshort || s.country_short || '').trim(),
		config_b64: String(s.openvpn_configdata_base64 || s.config_b64 || '').trim(),
	})).filter(r => r.host && r.ip);
}

// ---------------------------------------------------------------------------
// 筛 SSTP
// ---------------------------------------------------------------------------
const RE_PROTO_TCP = /^\s*proto\s+(tcp|tcp4|tcp6)\b/im;
const RE_REMOTE = /^\s*remote\s+(\S+)\s+(\d+)/im;

function extractSstpNodes(rows) {
	const out = [];
	for (const r of rows) {
		let cfg = '';
		if (r.config_b64) {
			try { cfg = Buffer.from(r.config_b64, 'base64').toString('utf8'); } catch (e) { continue; }
		}
		if (!RE_PROTO_TCP.test(cfg)) continue;
		const m = RE_REMOTE.exec(cfg);
		if (!m) continue;
		const port = Number(m[2]);
		if (!(port >= 1 && port <= 65535)) continue;
		let host = r.host;
		if (!host.endsWith('.opengw.net')) host = `${host}.opengw.net`;
		out.push({ host, port, ip: r.ip, country: r.country_long, code: r.country_short });
	}
	return out;
}

function dedupe(nodes) {
	const seen = new Set();
	const out = [];
	for (const n of nodes) {
		const key = `${n.host.toLowerCase()}:${n.port}`;
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(n);
	}
	return out;
}

// ---------------------------------------------------------------------------
// 探测
// ---------------------------------------------------------------------------
async function checkAll(nodes) {
	const results = new Array(nodes.length);
	let cursor = 0;
	const workers = Array.from({ length: Math.min(CONFIG.CONCURRENCY, nodes.length) }, async () => {
		while (cursor < nodes.length) {
			const i = cursor++;
			results[i] = await checkOne(nodes[i]);
		}
	});
	await Promise.all(workers);
	return results;
}

async function checkOne(node) {
	const base = { ...node, checked_at: now() };
	let probe = null;
	let via = 'local';

	// 优先走云端：延迟反映 CF 边缘到客户端的真实链路
	if (CONFIG.CHECK_WORKER) {
		try {
			probe = await probeViaWorker(node);
			via = 'worker';
		} catch (e) {
			if (!CONFIG.WORKER_FALLBACK) {
				return { ...base, success: false, error: `worker: ${e.message}`, exit: null, resident: classify(node.host, '') };
			}
			log('云端检测失败，退回本地', `${node.host}:${node.port} - ${e.message}`);
		}
	}

	if (!probe) {
		try {
			probe = await probeLocal(node);
			via = 'local';
		} catch (e) {
			return { ...base, success: false, error: e.message, exit: null, resident: classify(node.host, '') };
		}
	}

	const resident = classify(node.host, probe.exit?.org || '');
	const finalResident = probe.exit?.type || resident;
	return { ...base, ...probe, via, resident: finalResident };
}

async function probeViaWorker(node) {
	const url = `${CONFIG.CHECK_WORKER}${encodeURIComponent(`vpn:vpn@${node.host}:${node.port}`)}`;
	const r = await fetch(url, { signal: AbortSignal.timeout(CONFIG.TIMEOUT_MS + 10000) });
	if (!r.ok) throw new Error(`HTTP ${r.status}`);
	const j = await r.json();
	// Worker 内部已做 ASN 二次查询，exit 里带 type，直接沿用
	return {
		success: !!j.success,
		latency_ms: j.responseTime ?? null,
		colo: j.colo || null,
		exit: j.exit || null,
		assigned_ip: j.assigned_ip || null,
		error: j.success ? null : (j.error || 'worker check failed'),
	};
}

async function probeLocal(node) {
	const t0 = Date.now();
	const res = await sstpProbe({
		host: node.host,
		port: node.port,
		username: 'vpn',
		password: 'vpn',
		targetHost: 'ip-api.com',
		targetPort: 80,
		path: '/json/?fields=status,country,countryCode,city,isp,org,as,query',
		dial,
		resolve: resolveA,
		timeout: CONFIG.TIMEOUT_MS,
	});
	let exit = null;
	try {
		const b = JSON.parse(res.body);
		if (b && b.query) {
			exit = {
				ip: b.query,
				country: b.country,
				country_code: b.countryCode,
				city: b.city,
				org: b.isp || b.org || '',
				asn: b.as || '',
			};
		}
	} catch (e) { /* 响应不是 JSON */ }

	// 隧道里拿到的 isp 通常是 SoftEther 本身，用出口 IP 再查一次真实归属
	if (exit?.ip) {
		const asn = await lookupExitAsn(exit.ip);
		if (asn) {
			exit = {
				...exit,
				org: asn.isp || asn.org || exit.org,
				asn: asn.as || exit.asn,
				country: asn.country || exit.country,
				country_code: asn.countryCode || exit.country_code,
				city: asn.city || exit.city,
				type: classifyExit(asn.isp || asn.org || ''),
			};
		}
	}

	return {
		success: res.success && !!exit,
		latency_ms: Date.now() - t0,
		assigned_ip: res.assigned_ip,
		exit,
		error: res.success ? (exit ? null : 'no exit info') : (res.status_line || 'no response'),
	};
}

// 出口 ASN 二次查询：SSTP 隧道里看到的 isp 往往就是 SoftEther 本身
async function lookupExitAsn(ip) {
	if (!ip) return null;
	try {
		const r = await fetch(`http://ip-api.com/json/${encodeURIComponent(ip)}?fields=status,country,countryCode,city,isp,org,as`, {
			signal: AbortSignal.timeout(5000),
		});
		const j = await r.json();
		return j.status === 'success' ? j : null;
	} catch (e) {
		return null;
	}
}

function classify(host, org) {
	return classifyExit(org);
}

// ---------------------------------------------------------------------------
// 输出
// ---------------------------------------------------------------------------
async function writeOutputs(results) {
	const ok = results.filter(r => r.success)
		.sort((a, b) => (a.latency_ms ?? 1e9) - (b.latency_ms ?? 1e9));

	await mkdir(CONFIG.PUBLIC_DIR, { recursive: true });

	const data = buildData(results, ok);
	await writeFile(join(CONFIG.PUBLIC_DIR, 'data.json'), JSON.stringify(data, null, 1));
	await writeFile(join(CONFIG.PUBLIC_DIR, 'nodes.txt'), buildNodesTxt(ok));
	await writeFile(join(CONFIG.PUBLIC_DIR, 'nodes.json'), JSON.stringify(buildPool(ok), null, 1));
	log(`写出 ${join(CONFIG.PUBLIC_DIR, 'data.json')} / nodes.txt / nodes.json（可用 ${ok.length}）`);
}

// 供 Worker/Vercel 读取的节点池。字段由 core/schema.js 统一决定，
// 新增字段只改那一处，这里不再维护白名单。
function buildPool(ok) {
	return sortByLatency(ok).map(n => projectNode(n));
}

function buildData(results, ok) {
	// 展示层的字段比池多（protocol / country_code / residential / link 别名），
	// 一律由 core/schema.js 的 VIEW_FIELDS 决定。
	const viewNodes = ok.map(n => attachComputed(projectNode(n, VIEW_FIELDS)));

	return {
		generated_at: now(),
		source: 'vpngate-edge',
		// 与 Worker 动态生成的 /data.json 同构：客户端可以据此判断池是否过期
		pool_rev: poolRev(viewNodes),
		age_minutes: poolAgeMinutes(viewNodes),
		pool_source: 'pipeline',
		stats: buildStats(viewNodes, { checked: results.length }),
		countries: groupByCountry(viewNodes),
		available: viewNodes,
	};
}

function buildNodesTxt(ok) {
	return ok.map((n, i) => {
		const tag = `${n.code || 'SSTP'}-${n.resident === 'residential' ? '住宅' : n.resident === 'datacenter' ? '机房' : '未知'}-${String(i + 1).padStart(2, '0')}`;
		return `${n.host}:${n.port}#${tag}$sstp://vpn:vpn@${n.host}:${n.port}`;
	}).join('\n') + (ok.length ? '\n' : '');
}

async function pushToKv(ok) {
	const url = CONFIG.KV_REST_URL || (CONFIG.CF_ACCOUNT_ID && CONFIG.CF_KV_ID
		? `https://api.cloudflare.com/client/v4/accounts/${CONFIG.CF_ACCOUNT_ID}/storage/kv/namespaces/${CONFIG.CF_KV_ID}/values/nodes`
		: '');

	if (!url) { log('KV 跳过（未配置 CF_KV_ID / KV_REST_URL）'); return; }
	if (!CONFIG.CF_API_TOKEN) { log('KV 跳过（未配置 CF_API_TOKEN）'); return; }

	// 单调守卫：新快照的 checked_at 不得早于 KV 里已有的，
	// 避免重试任务 / 慢任务把旧数据盖回来。那样所有客户端会静默
	// 回退到更旧的池，且没有任何报错。
	const incomingRev = poolRev(ok);
	try {
		const cur = await fetch(url, {
			headers: { authorization: `Bearer ${CONFIG.CF_API_TOKEN}` },
			signal: AbortSignal.timeout(8000),
		});
		if (cur.ok) {
			const prev = projectPool(await cur.json());
			if (prev.length && poolRev(prev) === incomingRev) {
				log('KV 跳过（内容未变）', incomingRev);
				return;
			}
			const prevAge = poolAgeMinutes(prev);
			const nextAge = poolAgeMinutes(ok);
			if (prevAge != null && nextAge != null && nextAge > prevAge + 5) {
				log('KV 拒绝写入：新快照更旧', `旧 ${prevAge}min → 新 ${nextAge}min`);
				return;
			}
		}
	} catch (e) {
		log('KV 读取失败（继续写入）:', e.message);
	}

	// 存投影后的完整节点池（带 nodeId / latency / resident / checked_at），
	// 而不只是 host/port——这样 Worker 的 /api/nodes、订阅、后台都能直接用。
	const payload = JSON.stringify(projectPool(ok));

	const r = await fetch(url, {
		method: 'PUT',
		headers: { authorization: `Bearer ${CONFIG.CF_API_TOKEN}`, 'content-type': 'text/plain' },
		body: payload,
	});
	log('KV 写入', r.status, r.ok ? `ok（${ok.length} 个节点，rev=${incomingRev}）` : await r.text());
}
