// 平台无关的 VLESS-over-WebSocket -> 上游 TCP 转发
//
// 适配层提供 ws 抽象：
//   ws.onMessage((u8) => void)
//   ws.onClose(() => void)
//   ws.send(u8)
//   ws.close()
//   ws.drainEarlyData() => Uint8Array[]   可选：握手头里携带的 early data
// 以及 openUpstream(hostname, port) => { readable, writable, close }
//
// 关于 early data：Shadowrocket / sing-box / v2rayN 等客户端会把 VLESS 首包
// 塞进 WebSocket 握手的 Sec-WebSocket-Protocol 头（"?ed=2048" 就是开启它的约定）。
// 这些字节不会以 message 事件到达，必须从握手头里取出来，否则服务端永远等不到
// 首包，表现为「连上了但没有任何响应，最后超时」。

import { parseVlessRequest, vlessResponseHeader } from './vless.js';
import { toU8, concat } from './bytes.js';

export function createVlessSession({ uuid, openUpstream, ws, onLog = () => { } }) {
	const state = {
		buffer: new Uint8Array(0),
		parsed: false,
		closed: false,
		writer: null,
		socket: null,
	};

	const shutdown = () => {
		if (state.closed) return;
		state.closed = true;
		try { state.socket?.close?.(); } catch (e) { }
		try { ws.close(); } catch (e) { }
	};

	if (!uuid) {
		onLog('缺少 UUID，拒绝连接');
		shutdown();
		return { shutdown };
	}

	// 统一的「喂数据」入口：early data 与后续 message 走同一条路径
	async function feed(raw) {
		if (state.closed) return;
		const chunk = toU8(raw);
		if (!chunk.byteLength) return;

		// 建立后再来的都是净荷
		if (state.parsed) {
			try {
				await state.writer.write(chunk);
			} catch (e) {
				shutdown();
			}
			return;
		}

		state.buffer = state.buffer.byteLength ? concat(state.buffer, chunk) : chunk;
		const req = parseVlessRequest(state.buffer, uuid);
		if (!req.ok) {
			if (req.needMore) return;
			onLog(`VLESS 解析失败: ${req.error}`);
			shutdown();
			return;
		}
		if (req.isUDP) {
			onLog('暂不支持 VLESS UDP');
			shutdown();
			return;
		}

		state.parsed = true;
		state.buffer = new Uint8Array(0);

		try {
			const socket = await openUpstream(req.hostname, req.port);
			if (state.closed) { try { socket.close?.(); } catch (e) { } return; }
			state.socket = socket;
			state.writer = socket.writable.getWriter();

			ws.send(vlessResponseHeader());
			if (req.payload.byteLength) await state.writer.write(req.payload);

			// 下行
			(async () => {
				const reader = socket.readable.getReader();
				try {
					for (; ;) {
						const { value, done } = await reader.read();
						if (done) break;
						if (value?.byteLength) ws.send(value);
					}
				} catch (e) {
					// 隧道错误
				} finally {
					try { reader.releaseLock(); } catch (e) { }
					shutdown();
				}
			})();
		} catch (e) {
			onLog(`上游连接失败 ${req.hostname}:${req.port} - ${e?.message || e}`);
			shutdown();
		}
	}

	ws.onMessage((raw) => { feed(raw); });

	// 握手头里带的 early data，必须先喂进去
	if (typeof ws.drainEarlyData === 'function') {
		try {
			const chunks = ws.drainEarlyData() || [];
			if (chunks.length) {
				onLog(`收到 early data ${chunks.reduce((n, c) => n + (c?.byteLength || 0), 0)} 字节`);
				for (const c of chunks) feed(c);
			}
		} catch (e) {
			onLog(`early data 处理失败: ${e?.message || e}`);
		}
	}

	ws.onClose(shutdown);
	return { shutdown };
}
