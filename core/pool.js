// 节点池加载（Worker / Vercel / 自测共用）
//
// 回落顺序：
//   1. SSTP_POOL 环境变量（调试用，立即生效）
//   2. KV 的 nodes key（Cloudflare 原生，毫秒级）
//   3. POOL_URL（GitHub Pages 等外链；Vercel 没有 KV，靠它拿新鲜数据）
//   4. 打包进来的静态 nodes.json（兜底，可能冻结在上次部署）
//
// 每次回落链都带 TTL 记忆 + single-flight：热路径不重复发网络请求，
// 并发建连只放一个请求出去。TTL 到期后重新走一遍链路，因此长驻进程
// 不会把池冻结在启动那一刻。

import { projectPool, poolRev, poolAgeMinutes } from './schema.js';

const DEFAULT_TTL_MS = 90_000;

const memo = { pool: null, rev: '', at: 0, age: null, source: 'empty' };
let inflight = null;

/** 清空缓存。测试与「刚推完 KV 想立刻生效」时用。 */
export function resetPoolCache() {
	memo.pool = null; memo.rev = ''; memo.at = 0; memo.age = null; memo.source = 'empty';
	inflight = null;
}

async function readKv(env) {
	if (!env?.KV) return null;
	try {
		const arr = projectPool(await env.KV.get('nodes', 'json'));
		return arr.length ? arr : null;
	} catch (e) {
		return null;
	}
}

async function readUrl(cfg) {
	if (!cfg.POOL_URL) return null;
	try {
		const res = await fetch(cfg.POOL_URL, {
			headers: { accept: 'application/json' },
			signal: AbortSignal.timeout(cfg.POOL_FETCH_TIMEOUT_MS || 4000),
		});
		if (!res.ok) return null;
		const arr = projectPool(await res.json());
		return arr.length ? arr : null;
	} catch (e) {
		return null;
	}
}

async function readStatic(env) {
	// Cloudflare 适配层注入的读取器（worker/src/index.js 的 withStaticReader）
	if (typeof env?.__readStatic === 'function') {
		try {
			const arr = projectPool(await env.__readStatic('nodes.json'));
			if (arr.length) return arr;
		} catch (e) { /* 文件不存在 */ }
	}
	// Cloudflare ASSETS 绑定
	if (env?.ASSETS) {
		try {
			const res = await env.ASSETS.fetch(new Request('https://assets/nodes.json'));
			if (res.ok) {
				const arr = projectPool(await res.json());
				if (arr.length) return arr;
			}
		} catch (e) { /* 忽略 */ }
	}
	// Vercel：public/nodes.json（适配层注入的读取器，返回字符串或已解析对象）
	if (typeof env?.__readFile === 'function') {
		try {
			const raw = await env.__readFile('nodes.json');
			const arr = projectPool(typeof raw === 'string' ? JSON.parse(raw) : raw);
			if (arr.length) return arr;
		} catch (e) { /* 忽略 */ }
	}
	return null;
}

/**
 * 读池。返回 { pool, rev, age_minutes, source }，绝不抛异常。
 * @param {object} env 平台环境变量 / 绑定
 * @param {object} cfg readConfig() 的结果
 */
export async function loadPoolState(env = {}, cfg = {}) {
	const ttl = Number(cfg.POOL_CACHE_TTL_MS ?? DEFAULT_TTL_MS);
	const now = Date.now();

	if (memo.pool && now - memo.at < ttl) {
		return {
			pool: memo.pool,
			rev: memo.rev,
			age_minutes: poolAgeMinutes(memo.pool, now),
			source: 'memo',
		};
	}
	// single-flight：并发请求共享同一次加载
	if (inflight) return inflight;

	inflight = (async () => {
		let pool = null;
		let source = 'empty';

		if (cfg.SSTP_POOL) {
			const fromEnv = parsePoolSpec(cfg.SSTP_POOL);
			if (fromEnv.length) { pool = projectPool(fromEnv); source = 'env'; }
		}
		if (!pool?.length) {
			const kv = await readKv(env);
			if (kv?.length) { pool = kv; source = 'kv'; }
		}
		if (!pool?.length) {
			const url = await readUrl(cfg);
			if (url?.length) { pool = url; source = 'url'; }
		}
		if (!pool?.length) {
			const st = await readStatic(env);
			if (st?.length) { pool = st; source = 'static'; }
		}

		// 全链路都失败：宁可返回上一份快照，也别让用户拿到空池。
		// 同时刷新 memo.at，避免每个请求都把整条回落链再打一遍（最坏 4 次超时）。
		if (!pool?.length) {
			if (memo.pool) {
				memo.at = Date.now();
				return { pool: memo.pool, rev: memo.rev, age_minutes: poolAgeMinutes(memo.pool), source: 'stale' };
			}
			return { pool: [], rev: 'empty', age_minutes: null, source: 'empty' };
		}

		const rev = poolRev(pool);
		memo.pool = pool; memo.rev = rev; memo.at = Date.now(); memo.source = source;
		return { pool, rev, age_minutes: poolAgeMinutes(pool), source };
	})();

	try {
		return await inflight;
	} finally {
		inflight = null;
	}
}

/** 只要池数组（大多数调用点用这个） */
export async function loadPool(env, cfg) {
	return (await loadPoolState(env, cfg)).pool;
}

/**
 * 解析 SSTP_POOL 环境变量：逗号分隔，支持 `sstp://user:pass@host:port`、
 * `host:port`、`host`（默认 443）三种写法。
 */
export function parsePoolSpec(raw) {
	if (!raw) return [];
	return String(raw).split(',').map(s => s.trim()).filter(Boolean).map((s) => {
		const m = /^(?:sstp:\/\/)?(?:[^@/]*@)?([^:/]+)(?::(\d+))?$/.exec(s);
		if (m) return { host: m[1], port: Number(m[2] || 443) };
		return { host: s, port: 443 };
	}).filter(n => n.host);
}
