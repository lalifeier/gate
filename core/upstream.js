// 平台无关的上游选择：direct / SSTP 链式（池内故障转移 + 被动摘除）
import { sstpConnect } from './sstp.js';
import { recordFailure, recordSuccess, orderCandidates, resetEjection, ejectedCount } from './health.js';
import { findById } from './schema.js';

/**
 * @param {object} p
 * @param {object} p.cfg           readConfig() 的结果
 * @param {Function} p.dial        (host, port, opts) => socket 形状
 * @param {Function} p.resolve     (host) => ipv4
 * @param {Function} p.pickNodes   () => Promise<Array<{host, port, nodeId}>>
 * @param {string} [p.pinnedId]    通过 ?node=<nodeId> 固定的节点，找不到即报错
 * @param {number} [p.pinnedIndex] 旧订阅的 ?node=N（排序下标），仅向后兼容
 */
export function createUpstreamOpener({ cfg, dial, resolve, pickNodes, pinnedId = null, pinnedIndex = -1, onLog = () => { } }) {
	return async function openUpstream(hostname, port) {
		if (cfg.CHAIN === 'direct') {
			return dial(hostname, port, {});
		}

		const pool = await pickNodes();
		if (!pool.length) {
			if (cfg.CHAIN === 'fallback') return dial(hostname, port, {});
			throw new Error('节点池为空，且 CHAIN=always');
		}

		// 固定模式：优先按内容寻址 ID 精确匹配；旧订阅的纯数字下标走下面这条
		const pinned = pinnedId
			? findById(pool, pinnedId)
			: (Number.isInteger(pinnedIndex) && pinnedIndex >= 0 ? pool[pinnedIndex] : null);
		const wantPin = !!pinnedId || (Number.isInteger(pinnedIndex) && pinnedIndex >= 0);

		if (wantPin) {
			if (!pinned) {
				throw new Error(pinnedId
					? `固定节点 ${pinnedId} 不存在（池中 ${pool.length} 个）`
					: `节点下标越界：${pinnedIndex}（池中 ${pool.length} 个）`);
			}
			try {
				onLog(`固定节点 ${pinned.host}:${pinned.port} -> ${hostname}:${port}`);
				return await connectSstp(pinned, hostname, port, cfg, dial, resolve);
			} catch (e) {
				onLog(`固定节点失败 ${pinned.host}:${pinned.port} - ${e?.message || e}`);
				recordFailure(pinned);
				if (cfg.CHAIN === 'fallback') return dial(hostname, port, {});
				throw e;
			}
		}

		// 自动轮换：先排除被动摘除中的节点（含 50% 上限保护）
		const order = orderCandidates(pool);
		const maxTries = Math.min(order.length, Number(cfg.SSTP_MAX_TRIES || 3));
		const start = Math.floor(Date.now() / 300000) % order.length;

		let lastErr = null;
		for (let i = 0; i < maxTries; i++) {
			const node = order[(start + i) % order.length];
			try {
				onLog(`经 SSTP ${node.host}:${node.port} -> ${hostname}:${port}（${i + 1}/${maxTries}）`);
				return await connectSstp(node, hostname, port, cfg, dial, resolve);
			} catch (e) {
				lastErr = e;
				onLog(`SSTP 失败 ${node.host}:${node.port} - ${e?.message || e}`);
				recordFailure(node);
			}
		}

		if (cfg.CHAIN === 'fallback') {
			onLog('全部 SSTP 节点失败，降级直连');
			return dial(hostname, port, {});
		}
		throw lastErr || new Error('所有 SSTP 节点均失败');
	};
}

/** 单次 SSTP 建连；成功清一次冷却，失败抛出原错误 */
async function connectSstp(node, hostname, port, cfg, dial, resolve) {
	const conn = await sstpConnect({
		host: node.host,
		port: node.port,
		username: cfg.SSTP_USER,
		password: cfg.SSTP_PASS,
		targetHost: hostname,
		targetPort: port,
		dial,
		resolve,
		timeout: cfg.SSTP_TIMEOUT_MS,
	});
	// 连接建立即代表一次成功的拨号，清除被动摘除
	recordSuccess(node);
	return conn;
}

/** 解析订阅路径里的 ?node=N（数字下标）或 ?node=<id>（内容寻址）。
 *  数字下标仅为兼容旧订阅链接保留；新订阅一律用 nodeId。
 *  返回 { pinnedId, pinnedIndex }。
 */
export function parseNodePin(raw) {
	if (raw === null || raw === undefined || raw === '') return { pinnedId: null, pinnedIndex: -1 };
	// 纯数字 → 数字下标（仅为旧订阅兜底）
	if (/^\d+$/.test(raw)) {
		const n = Number(raw);
		return Number.isInteger(n) && n >= 0 ? { pinnedId: null, pinnedIndex: n } : { pinnedId: null, pinnedIndex: -1 };
	}
	// 其余按 nodeId 精确匹配（形如 n3f2a1b9c8）
	return { pinnedId: raw, pinnedIndex: -1 };
}

export { resetEjection, ejectedCount };
