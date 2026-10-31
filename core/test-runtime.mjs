// 验证「数据面」的三件事：
//   1. 节点 ID 是内容寻址的：排序变化不改变 ID
//   2. 被动摘除：失败进入冷却、成功清除、全池坏掉时不摘空
//   3. 池加载优先级与单调版本号
//
//   node core/test-runtime.mjs

import { nodeIdOf, poolRev, poolAgeMinutes, projectPool, projectNode } from './schema.js';
import { isEjected, recordFailure, recordSuccess, orderCandidates, ejectedCount, resetEjection } from './health.js';
import { loadPoolState, parsePoolSpec, resetPoolCache } from './pool.js';
import { parseNodePin } from './upstream.js';

let failed = 0;
const ok = (cond, label, extra = '') => {
	console.log(cond ? `  ✓ ${label}` : `  ✗ ${label}`, extra);
	if (!cond) failed++;
};

// ---------------------------------------------------------------------------
console.log('[1] 节点 ID 内容寻址');
{
	const a = nodeIdOf('public-vpn-94.opengw.net', 443);
	const b = nodeIdOf('PUBLIC-VPN-94.opengw.net', 443);
	const c = nodeIdOf('public-vpn-94.opengw.net', 1822);
	ok(a === b, 'host 大小写不影响 ID', a);
	ok(a !== c, '端口不同 → ID 不同', `${a} vs ${c}`);
	ok(/^n[0-9a-f]{11}$/.test(a), 'ID 形如 n + 11 位 hex', a);

	// 关键回归：排序变化不能让固定链接漂移
	const pool = [
		{ host: 'a.opengw.net', port: 443, latency_ms: 900 },
		{ host: 'b.opengw.net', port: 443, latency_ms: 100 },
		{ host: 'c.opengw.net', port: 443, latency_ms: 500 },
	].map(n => projectNode(n));
	const before = pool.map(n => n.nodeId).sort().join(',');
	const resorted = [...pool].sort((x, y) => x.latency_ms - y.latency_ms).map(n => n.nodeId).sort().join(',');
	ok(before === resorted, '重排序后 ID 集合不变');
	ok(pool[0].nodeId === nodeIdOf('a.opengw.net', 443), 'projectNode 自动补 nodeId');
}

// ---------------------------------------------------------------------------
console.log('\n[2] 被动摘除');
{
	resetEjection();
	const pool = ['a', 'b', 'c', 'd'].map(h => ({ host: `${h}.opengw.net`, port: 443 }));

	ok(!isEjected(pool[0]), '初始不在冷却');

	const rec = recordFailure(pool[0]);
	ok(isEjected(pool[0]), '失败一次即冷却');
	ok(rec.until - Date.now() > 20_000, '冷却约 30s（含抖动）', `${Math.round((rec.until - Date.now()) / 1000)}s`);

	recordFailure(pool[0]);
	const rec2 = isEjected(pool[0]);
	ok(rec2, '再次失败仍在冷却');

	recordSuccess(pool[0]);
	ok(!isEjected(pool[0]), '成功一次立即回池');
	ok(ejectedCount() === 0, '冷却计数归零');

	// 顺序：活的在前
	recordFailure(pool[1]);
	const order = orderCandidates(pool);
	ok(order[0].host !== 'b.opengw.net', '被摘除的排到最后', order.map(n => n.host).join(' > '));
	ok(order[order.length - 1].host === 'b.opengw.net', '坏节点仍在候选里（不是直接删掉）');

	// 摘空保护：超过一半被摘 → 摘除失效
	resetEjection();
	recordFailure(pool[0]); recordFailure(pool[1]); recordFailure(pool[2]);
	const guarded = orderCandidates(pool);
	ok(guarded.length === pool.length, '摘除超过 50% 时全部放回', `${guarded.length}/${pool.length}`);

	resetEjection();
}

// ---------------------------------------------------------------------------
console.log('\n[3] 池加载优先级');
{
	resetPoolCache();
	const env = {
		KV: {
			get: async () => [{ host: 'kv.opengw.net', port: 443 }],
		},
	};
	const cfgEmpty = {};
	const kvOnly = await loadPoolState(env, cfgEmpty);
	ok(kvOnly.source === 'kv' && kvOnly.pool[0].host === 'kv.opengw.net', 'KV 优先于静态');

	// 环境变量优先级最高
	resetPoolCache();
	const withEnv = await loadPoolState(env, { SSTP_POOL: 'env.opengw.net:1822' });
	ok(withEnv.source === 'env' && withEnv.pool[0].port === 1822, 'SSTP_POOL 优先于 KV');

	// 没有 KV 时回落到 __readFile
	resetPoolCache();
	const fileEnv = { __readFile: async () => JSON.stringify([{ host: 'file.opengw.net', port: 443 }]) };
	const fromFile = await loadPoolState(fileEnv, {});
	ok(fromFile.source === 'static' && fromFile.pool[0].host === 'file.opengw.net', '静态文件兜底');

	// 全空 → 返回上一份快照，而不是空池（先加载一次有数据的，再打满空 env）
	// TTL=0 是为了绕过缓存命中，真的走一遍全链路
	resetPoolCache();
	await loadPoolState(env, {});
	const stale = await loadPoolState({}, { POOL_CACHE_TTL_MS: 0 });
	ok(stale.source === 'stale' && stale.pool.length === 1, '链路全失败时回退上一份快照');

	// 缓存命中不再走链路
	resetPoolCache();
	await loadPoolState(env, {});
	const hit = await loadPoolState(env, {});
	ok(hit.source === 'memo', 'TTL 内命中缓存');

	ok(parsePoolSpec('a:443, b, sstp://vpn:vpn@c:1822').length === 3, 'parsePoolSpec 接受三种写法');
	ok(parsePoolSpec('a:443')[0].port === 443 && parsePoolSpec('b')[0].port === 443, '缺省端口 443');
	resetPoolCache();
}

// ---------------------------------------------------------------------------
console.log('\n[4] 单调版本号与新鲜度');
{
	const older = projectPool([{ host: 'a.opengw.net', port: 443, checked_at: '2026-10-30T10:00:00Z' }]);
	const newer = projectPool([{ host: 'a.opengw.net', port: 443, checked_at: '2026-10-30T11:00:00Z' }]);
	ok(poolRev(older) !== poolRev(newer), '不同 checked_at → 不同 rev');
	ok(poolRev(older) === poolRev(projectPool([{ host: 'a.opengw.net', port: 443, checked_at: '2026-10-30T10:00:00Z' }])), '内容一致 → rev 稳定');

	const age = poolAgeMinutes(newer, Date.parse('2026-10-30T11:20:00Z'));
	ok(age === 20, 'poolAgeMinutes 算得对', `${age}min`);
	ok(poolAgeMinutes([{ host: 'x.opengw.net' }]) === null, '没有 checked_at 时返回 null');
}

// ---------------------------------------------------------------------------
console.log('\n[5] ?node 解析');
{
	ok(parseNodePin(null).pinnedId === null, '空值 → 自动轮换');
	ok(parseNodePin('2').pinnedIndex === 2, '纯数字 → 旧下标');
	ok(parseNodePin('n3f2a1b9c8d').pinnedId === 'n3f2a1b9c8d', 'nodeId → 内容寻址');
	ok(parseNodePin('n3f2a1b9c8d').pinnedIndex === -1, 'nodeId 不当作下标');
}

console.log(failed ? `\n失败 ${failed} 项` : '\n全部通过');
process.exit(failed ? 1 : 0);
