// 节点字段的唯一定义
//
// 探测结果 → nodes.json → /data.json → 页面，这条链路上一处都按这里定义。
// 新增字段只改 POOL_FIELDS / VIEW_FIELDS。
//
// ⚠ 不要在其它文件手写字段白名单——那样容易漏改，页面会某列一直是空。


/**
 * 稳定节点 ID：内容寻址（host:port），与池内排序无关。
 *
 * 订阅用它固定节点。不能用「排序后的下标」——池子每轮刷新都会重排，
 * 同一个下标会指向另一个节点，客户端无从察觉。
 *
 * 非加密哈希（FNV-1a + 乘法混合），只要求稳定、够短、碰撞率可忽略：
 * 48 bit，200 个节点的碰撞概率约 7e-11。同步实现，因为 projectNode 是同步的。
 *
 * 前缀 `n` 是必需的：纯十六进制有约 0.35% 的概率恰好全是数字，
 * 会被 parseNodePin 误判成数字下标。加了前缀就永不歧义。
 */
export function nodeIdOf(host, port) {
	const s = `${String(host || '').toLowerCase()}:${Number(port) || 443}`;
	let h1 = 0x811c9dc5;
	let h2 = 0x01000193;
	for (let i = 0; i < s.length; i++) {
		const c = s.charCodeAt(i);
		h1 = Math.imul(h1 ^ c, 0x01000193) >>> 0;
		h2 = Math.imul(h2 + c, 0x85ebca6b) >>> 0;
		h2 = (h2 ^ (h2 >>> 13)) >>> 0;
	}
	return 'n' + (h1.toString(16).padStart(8, '0') + h2.toString(16).padStart(8, '0')).slice(0, 11);
}

/**
 * 节点池字段定义（nodes.json / KV / ASSETS）。
 * key: 输出字段名；from: 源对象字段名（缺省同 key）；default: 缺失时的兜底。
 */
export const POOL_FIELDS = {
	host: { required: true, default: '' },
	port: { coerce: v => Number(v) || 443, default: 443 },
	nodeId: { default: null },       // 缺省由 nodeIdOf(host, port) 生成，见 projectNode
	ip: { default: null },
	country: { default: '' },
	code: { coerce: v => (v || '').toString().toUpperCase(), default: '' },
	tag: { default: null },          // 缺省回退到 host
	latency_ms: { default: null },
	resident: { default: 'unknown' },
	exit: { default: null },
	assigned_ip: { default: null },
	colo: { default: null },
	via: { default: null },
	checked_at: { default: null },
	success: { default: true },
	error: { default: null },
};

/** 展示层字段（/data.json → 页面）。在池字段基础上增加页面直接读取的别名与派生字段。 */
export const VIEW_FIELDS = {
	...POOL_FIELDS,
	protocol: { default: 'sstp' },
	country_code: { from: 'code', default: '' },
	// 页面模板读 residential，池字段叫 resident，两个名字都保留
	residential: { from: 'resident', default: 'unknown' },
	link: { computed: true },        // 由 buildLink() 生成，依赖配置
};

/**
 * 按字段表把一个节点对象规范化。
 * @param {object} src 源对象
 * @param {object} table 字段表（POOL_FIELDS / VIEW_FIELDS）
 * @returns {object}
 */
export function projectNode(src, table = POOL_FIELDS) {
	const n = src || {};
	const out = {};
	for (const [key, def] of Object.entries(table)) {
		if (def.computed) continue;

		const raw = def.from ? n[def.from] : n[key];
		let v = raw === undefined || raw === null || raw === '' ? undefined : raw;

		if (v === undefined) v = def.default;
		if (def.coerce && v !== null && v !== undefined) {
			try { v = def.coerce(v); } catch (e) { v = def.default; }
		}

		// tag 没有值时回退到 host
		if (key === 'tag' && (v === null || v === undefined || v === '')) v = n.host || '';
		if (key === 'country_code' && !v) v = (n.code || '').toString().toUpperCase();
		if (key === 'nodeId' && !v) v = nodeIdOf(n.host, n.port);

		out[key] = v === undefined ? null : v;
	}
	return out;
}

/**
 * 附加 computed 字段。派生字段依赖运行时配置（如 SSTP 凭据），
 * 无法放进静态字段表，统一在这里生成，避免各处手写。
 */
export function attachComputed(node, { sstpUser = 'vpn', sstpPass = 'vpn' } = {}) {
	return {
		...node,
		link: `sstp://${sstpUser}:${sstpPass}@${node.host}:${node.port}`,
	};
}

/** 规范化一个节点池（接受数组 / {nodes:[]} / JSON 字符串） */
export function projectPool(raw, table = POOL_FIELDS) {
	let v = raw;
	if (typeof v === 'string') {
		try { v = JSON.parse(v); } catch (e) { return []; }
	}
	if (v && !Array.isArray(v) && Array.isArray(v.nodes)) v = v.nodes;
	if (!Array.isArray(v)) return [];

	return v
		.filter(n => n && typeof n.host === 'string' && n.host)
		.map(n => projectNode(n, table));
}

/** 池内排序：延迟升序，缺延迟的排最后 */
export function sortByLatency(nodes) {
	return [...nodes].sort((a, b) => (a.latency_ms ?? 1e9) - (b.latency_ms ?? 1e9));
}

/** 按稳定 ID 查找节点；找不到返回 null（调用方需显式报错，不能静默漂移） */
export function findById(pool, id) {
	if (!id) return null;
	return pool.find(n => n.nodeId === id) || null;
}

/**
 * 池的内容版本号。
 * 用于单调守卫：新快照的投票时间不得早于当前值，挡住重试任务 / 慢任务
 * 把旧数据盖回来。取「最新 checked_at + 节点数 + ID 集合哈希」，
 * 因此内容不变时版本号也不变，不会无谓地刷新缓存。
 */
export function poolRev(nodes) {
	let latest = '';
	let h = 0x811c9dc5;
	for (const id of (nodes || []).map(n => n.nodeId).filter(Boolean).sort()) {
		for (let i = 0; i < id.length; i++) h = Math.imul(h ^ id.charCodeAt(i), 0x01000193) >>> 0;
	}
	for (const n of nodes || []) {
		const t = String(n.checked_at || '');
		if (t > latest) latest = t;
	}
	return `${latest || 'na'}:${(nodes || []).length}:${h.toString(16).padStart(8, '0')}`;
}

/** 池的新鲜度（分钟）；没有 checked_at 时返回 null */
export function poolAgeMinutes(nodes, nowMs = Date.now()) {
	let latest = 0;
	for (const n of nodes || []) {
		const t = Date.parse(n.checked_at);
		if (Number.isFinite(t) && t > latest) latest = t;
	}
	return latest ? Math.floor((nowMs - latest) / 60000) : null;
}

/** 按国家分组，返回页面需要的分组结构 */export function groupByCountry(nodes) {
	const groups = {};
	for (const n of nodes) {
		const key = n.country || '未知';
		if (!groups[key]) groups[key] = { code: n.code || n.country_code || '', count: 0, residential: 0, datacenter: 0, nodes: [] };
		groups[key].nodes.push(n);
	}
	for (const g of Object.values(groups)) {
		g.nodes = sortByLatency(g.nodes);
		g.count = g.nodes.length;
		g.residential = g.nodes.filter(x => (x.residential || x.resident) === 'residential').length;
		g.datacenter = g.nodes.filter(x => (x.residential || x.resident) === 'datacenter').length;
	}
	return groups;
}

/** 统计信息，页面卡片直接读取 */
export function buildStats(nodes, { checked = nodes.length } = {}) {
	const res = nodes.filter(n => (n.residential || n.resident) === 'residential').length;
	const dc = nodes.filter(n => (n.residential || n.resident) === 'datacenter').length;
	return {
		sstp_nodes: nodes.length,
		checked,
		success: nodes.length,
		failed: Math.max(0, checked - nodes.length),
		countries: new Set(nodes.map(n => n.country || '未知')).size,
		residential_est: res,
		datacenter_est: dc,
		residential: res,
		datacenter: dc,
		unknown: nodes.length - res - dc,
		// 数据新鲜度：页面显示「最后更新」用的是 generated_at（请求时刻），
		// 真正决定节点是否还活的是 checked_at。两者差太多就说明池太久没刷新。
		age_minutes: poolAgeMinutes(nodes),
	};
}

