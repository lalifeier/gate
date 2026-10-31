// 被动摘除（outlier ejection）
//
// 思路来自 Envoy outlier_detection：不主动探测，而是把「建连时
// 本来就要付的那一次失败」记下来，冷却期内跳过这个节点。
// 这是唯一零额外成本的「实时可用性」信号。
//
//   连续失败 1 次 → 冷却 30s
//   再次失败      → 60s → 120s → 300s（上限）
//   成功一次      → 立即清除，回到池中
//   全池摘除超过 50% → 放弃摘除（防止把池子一起摘空）
//
// 状态是 isolate / 进程内的，不跨实例共享。跨实例同步属于大规模场景：
// 本地冷却已经能扣掉绝大多数重复撞墙。

const COOLDOWN_MS = [30_000, 60_000, 120_000, 300_000];
const MAX_EJECT_RATIO = 0.5;
const MAX_ENTRIES = 2000;     // 防止长期运行后 Map 无限增长

/** host:port → { fails, until, last } */
const state = new Map();

const keyOf = (node) => `${String(node.host).toLowerCase()}:${Number(node.port) || 443}`;

function prune(now) {
	if (state.size <= MAX_ENTRIES) return;
	for (const [k, v] of state) {
		if (v.until <= now) state.delete(k);
	}
	// 还是太多就丢掉最早的一批
	if (state.size > MAX_ENTRIES) {
		const extra = state.size - MAX_ENTRIES;
		let i = 0;
		for (const k of state.keys()) { if (i++ >= extra) break; state.delete(k); }
	}
}

/** 该节点当前是否处于冷却中 */
export function isEjected(node, now = Date.now()) {
	const rec = state.get(keyOf(node));
	if (!rec) return false;
	if (rec.until <= now) {
		state.delete(keyOf(node));   // 冷却到期，自动回池
		return false;
	}
	return true;
}

export function recordFailure(node) {
	const k = keyOf(node);
	const now = Date.now();
	const rec = state.get(k) || { fails: 0, until: 0, last: 0 };
	rec.fails += 1;
	rec.last = now;
	const base = COOLDOWN_MS[Math.min(rec.fails - 1, COOLDOWN_MS.length - 1)];
	// ±20% 抖动，避免同一时刻所有 isolate 同时把节点加回来
	const jitter = 1 + (Math.random() * 0.4 - 0.2);
	rec.until = now + Math.round(base * jitter);
	state.set(k, rec);
	prune(now);
	return rec;
}

export function recordSuccess(node) {
	state.delete(keyOf(node));
}

/**
 * 按「可尝试程度」重排候选：未被摘除的在前（保持传入顺序），
 * 被摘除的排最后——不是直接删掉，因为全池都坏时还得试。
 */
export function orderCandidates(pool, now = Date.now()) {
	const live = [];
	const dead = [];
	for (const n of pool) (isEjected(n, now) ? dead : live).push(n);

	// 摘除比例上限：超过一半就当摘除失效，把被摘除的也放回来
	if (pool.length && dead.length / pool.length > MAX_EJECT_RATIO) {
		return [...pool];
	}
	return [...live, ...dead];
}

/** 观测用：当前冷却中的节点数 */
export function ejectedCount(now = Date.now()) {
	let n = 0;
	for (const rec of state.values()) if (rec.until > now) n++;
	return n;
}

/** 测试用：清空所有冷却状态 */
export function resetEjection() {
	state.clear();
}
