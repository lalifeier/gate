// SSTP (PPP over HTTPS) 客户端 + 用户态 TCP/IP 栈 —— 纯 JS 实现，无 Node 依赖
// 协议实现依据 MS-SSTP 规范。
//
// 用法：
//   const conn = await sstpConnect({ host, port, username, password, targetHost, targetPort, dial, resolve });
//   conn.readable / conn.writable 是标准 Streams
//
// dial(hostname, port, { secure }) -> { opened, readable, writable, closed, close }
// resolve(hostname) -> ipv4 string

import {
	EMPTY, toU8, concat, u16, u32, randU16, randU32, checksum,
	withTimeout, isIPv4, stripBrackets,
} from './bytes.js';

const MSS = 1400;
const DEFAULT_TIMEOUT = 12000;

const PPP_LCP = 0xc021;
const PPP_PAP = 0xc023;
const PPP_IPCP = 0x8021;
const PPP_IPV4 = 0x0021;

function sstpControl(data) {
	const len = data.byteLength;
	const packet = new Uint8Array(4 + len);
	packet[0] = 0x10;
	packet[1] = 0x01;
	packet[2] = ((4 + len) >> 8) & 0x0f | 0x80;
	packet[3] = (4 + len) & 0xff;
	packet.set(data, 4);
	return packet;
}

function sstpData(pppFrame) {
	const len = 6 + pppFrame.byteLength;
	const packet = new Uint8Array(len);
	packet.set([0x10, 0x00, ((len >> 8) & 0x0f) | 0x80, len & 0xff, 0xff, 0x03]);
	packet.set(pppFrame, 6);
	return packet;
}

function buildPpp(protocol, code, id, options = []) {
	const optLen = options.reduce((n, o) => n + 2 + o.data.byteLength, 0);
	const frame = new Uint8Array(6 + optLen);
	const view = new DataView(frame.buffer);
	view.setUint16(0, protocol);
	frame[2] = code;
	frame[3] = id;
	view.setUint16(4, 4 + optLen);
	let off = 6;
	for (const o of options) {
		frame[off] = o.type;
		frame[off + 1] = 2 + o.data.byteLength;
		frame.set(o.data, off + 2);
		off += 2 + o.data.byteLength;
	}
	return frame;
}

function parsePpp(data) {
	const off = data.byteLength >= 2 && data[0] === 0xff && data[1] === 0x03 ? 2 : 0;
	if (data.byteLength - off < 4) return null;
	const protocol = u16(data, off);
	if (protocol === PPP_IPV4) return { protocol, ipPacket: data.subarray(off + 2) };
	if (data.byteLength - off < 6) return null;
	return {
		protocol,
		code: data[off + 2],
		id: data[off + 3],
		payload: data.subarray(off + 6),
		raw: data.subarray(off),
	};
}

function parseOptions(data) {
	const out = [];
	for (let off = 0; off + 2 <= data.byteLength;) {
		const len = data[off + 1];
		if (len < 2 || off + len > data.byteLength) break;
		out.push({ type: data[off], data: data.subarray(off + 2, off + len) });
		off += len;
	}
	return out;
}

/**
 * 建立一条经由 SSTP 服务器的 TCP 连接。
 * 返回 { readable, writable, closed, close }，与 Workers socket 形状一致。
 */
export async function sstpConnect({
	host, port = 443,
	username = 'vpn', password = 'vpn',
	targetHost, targetPort,
	dial, resolve,
	timeout = DEFAULT_TIMEOUT,
	onLog = () => { },
}) {
	let buffered = EMPTY;
	let pppId = 1;
	let socket = null, reader = null, writer = null;

	const close = () => {
		try { reader?.cancel?.().catch?.(() => { }); } catch (e) { }
		try { reader?.releaseLock?.(); } catch (e) { }
		try { writer?.close?.().catch?.(() => { }); } catch (e) { }
		try { writer?.releaseLock?.(); } catch (e) { }
		try { socket?.close?.(); } catch (e) { }
	};

	const readChunk = async () => {
		const { value, done } = await reader.read();
		if (done || !value) throw new Error('SSTP socket closed');
		return toU8(value);
	};
	const readBytes = async (n) => {
		while (buffered.byteLength < n) {
			const chunk = await readChunk();
			buffered = buffered.byteLength ? concat(buffered, chunk) : chunk;
		}
		const out = buffered.subarray(0, n);
		buffered = buffered.subarray(n);
		return out;
	};
	const readLine = async () => {
		for (; ;) {
			const i = buffered.indexOf(10);
			if (i >= 0) {
				const line = new TextDecoder().decode(buffered.subarray(0, i));
				buffered = buffered.subarray(i + 1);
				return line.replace(/\r$/, '');
			}
			const chunk = await readChunk();
			buffered = buffered.byteLength ? concat(buffered, chunk) : chunk;
		}
	};
	const readPacket = async (ms = timeout) => {
		const header = await withTimeout(readBytes(4), ms, 'SSTP read timeout');
		const len = u16(header, 2) & 0x0fff;
		if (len < 4) throw new Error('SSTP invalid packet length');
		return {
			isControl: (header[1] & 1) !== 0,
			body: len > 4 ? await withTimeout(readBytes(len - 4), ms, 'SSTP body read timeout') : EMPTY,
		};
	};
	const send = (frame) => withTimeout(writer.write(sstpData(frame)), timeout, 'SSTP write timeout');

	try {
		const serverHost = stripBrackets(host);
		socket = dial(serverHost, Number(port), { secure: true });
		await withTimeout(socket.opened, timeout, `SSTP server connect timed out (${serverHost}:${port})`);
		reader = socket.readable.getReader();
		writer = socket.writable.getWriter();

		const displayHost = serverHost.includes(':') ? `[${serverHost}]` : serverHost;
		const httpReq = new TextEncoder().encode(
			'SSTP_DUPLEX_POST /sra_{BA195980-CD49-458b-9E23-C84EE0ADCD75}/ HTTP/1.1\r\n'
			+ `Host: ${Number(port) === 443 ? displayHost : `${displayHost}:${port}`}\r\n`
			+ 'Content-Length: 18446744073709551615\r\n'
			+ `SSTPCORRELATIONID: {${crypto.randomUUID()}}\r\n\r\n`,
		);

		const encap = new Uint8Array(2);
		new DataView(encap.buffer).setUint16(0, 1);
		const mru = new Uint8Array(2);
		new DataView(mru.buffer).setUint16(0, 1500);

		const connectReq = new Uint8Array(12 + encap.byteLength);
		const cv = new DataView(connectReq.buffer);
		connectReq[0] = 0x10;
		connectReq[1] = 0x01;
		cv.setUint16(2, connectReq.byteLength | 0x8000);
		cv.setUint16(4, 0x0001);
		cv.setUint16(6, 1);
		connectReq[9] = 1;
		cv.setUint16(10, 4 + encap.byteLength);
		connectReq.set(encap, 12);

		await withTimeout(
			writer.write(concat(httpReq, connectReq, sstpData(buildPpp(PPP_LCP, 1, pppId++, [{ type: 1, data: mru }])))),
			timeout, 'SSTP handshake write timed out',
		);

		const status = await withTimeout(readLine(), timeout, 'SSTP handshake timeout');
		for (; ;) {
			const line = await withTimeout(readLine(), timeout, 'SSTP header read timeout');
			if (line === '') break;
		}
		if (!/HTTP\/\d(?:\.\d)?\s+2\d\d/i.test(status)) throw new Error(`SSTP handshake failed: ${status || 'no status'}`);

		// ---- PPP 协商 ----
		let localLcpAcked = false, peerLcpAcked = false;
		let papRequired = false, papSent = false, papDone = false;
		let ipcpStarted = false, ipcpFinished = false, sourceIp = null;

		const sendPapIfReady = async () => {
			if (!localLcpAcked || !peerLcpAcked || !papRequired || papSent) return;
			if (username == null || password == null) throw new Error('SSTP requires PAP auth');
			const u = new TextEncoder().encode(username);
			const p = new TextEncoder().encode(password);
			const len = 6 + u.byteLength + p.byteLength;
			const frame = new Uint8Array(2 + len);
			const v = new DataView(frame.buffer);
			v.setUint16(0, PPP_PAP);
			frame[2] = 1;
			frame[3] = pppId++;
			v.setUint16(4, len);
			frame[6] = u.byteLength;
			frame.set(u, 7);
			frame[7 + u.byteLength] = p.byteLength;
			frame.set(p, 8 + u.byteLength);
			await send(frame);
			papSent = true;
		};
		const startIpcpIfReady = async () => {
			if (!localLcpAcked || !peerLcpAcked || ipcpStarted || (papRequired && !papDone)) return;
			await send(buildPpp(PPP_IPCP, 1, pppId++, [{ type: 3, data: new Uint8Array(4) }]));
			ipcpStarted = true;
		};

		for (let round = 0; round < 50 && !ipcpFinished; round++) {
			const pkt = await readPacket();
			if (pkt.isControl) continue;
			const ppp = parsePpp(pkt.body);
			if (!ppp) continue;

			if (ppp.protocol === PPP_LCP) {
				if (ppp.code === 1) {
					const auth = parseOptions(ppp.payload).find(o => o.type === 3);
					if (auth?.data?.byteLength >= 2) {
						const proto = u16(auth.data);
						if (proto !== PPP_PAP) throw new Error(`SSTP auth protocol unsupported: 0x${proto.toString(16)}`);
						papRequired = true;
					}
					const ack = new Uint8Array(ppp.raw);
					ack[2] = 2;
					await send(ack);
					peerLcpAcked = true;
					await sendPapIfReady();
					await startIpcpIfReady();
				} else if (ppp.code === 2) {
					localLcpAcked = true;
					await sendPapIfReady();
					await startIpcpIfReady();
				}
				continue;
			}

			if (ppp.protocol === PPP_PAP) {
				if (ppp.code === 2) { papDone = true; await startIpcpIfReady(); }
				else if (ppp.code === 3) throw new Error('SSTP PAP authentication failed');
				continue;
			}

			if (ppp.protocol === PPP_IPCP) {
				if (ppp.code === 1) {
					const ack = new Uint8Array(ppp.raw);
					ack[2] = 2;
					await send(ack);
					await startIpcpIfReady();
				} else if (ppp.code === 3) {
					const addr = parseOptions(ppp.payload).find(o => o.type === 3);
					if (addr?.data?.byteLength === 4) {
						sourceIp = [...addr.data].join('.');
						await send(buildPpp(PPP_IPCP, 1, pppId++, [{ type: 3, data: addr.data }]));
						ipcpStarted = true;
					}
				} else if (ppp.code === 2) {
					const addr = parseOptions(ppp.payload).find(o => o.type === 3);
					if (addr?.data?.byteLength === 4) sourceIp = [...addr.data].join('.');
					ipcpFinished = true;
				}
			}
		}
		if (!sourceIp) throw new Error('SSTP did not provide an IPv4 address');

		// ---- 目标解析 ----
		const target = stripBrackets(targetHost);
		let targetIp = isIPv4(target) ? target : null;
		if (!targetIp) targetIp = await resolve(target);
		if (!targetIp) throw new Error(`Could not resolve ${targetHost} for SSTP`);

		// ---- 用户态 TCP/IP 栈 ----
		const sourcePort = 10000 + (randU16() % 50000);
		const srcAddr = new Uint8Array(sourceIp.split('.').map(Number));
		const dstAddr = new Uint8Array(targetIp.split('.').map(Number));
		let seq = randU32();
		let ack = 0;

		const ipTemplate = new Uint8Array(20);
		ipTemplate.set([0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 64, 6]);
		ipTemplate.set(srcAddr, 12);
		ipTemplate.set(dstAddr, 16);

		const pseudo = new Uint8Array(1432);
		pseudo.set(srcAddr);
		pseudo.set(dstAddr, 4);
		pseudo[9] = 6;

		const buildTcpFrame = (flags, payload = EMPTY) => {
			const body = toU8(payload);
			const tcpLen = 20 + body.byteLength;
			const ipLen = 20 + tcpLen;
			const frame = new Uint8Array(8 + ipLen);
			const view = new DataView(frame.buffer);
			frame.set([0x10, 0x00, ((8 + ipLen) >> 8) & 0x0f | 0x80, (8 + ipLen) & 0xff, 0xff, 0x03, 0x00, 0x21]);
			frame.set(ipTemplate, 8);
			view.setUint16(10, ipLen);
			view.setUint16(12, randU16());
			view.setUint16(18, checksum(frame, 8, 20));
			view.setUint16(28, sourcePort);
			view.setUint16(30, targetPort);
			view.setUint32(32, seq);
			view.setUint32(36, ack);
			frame[40] = 0x50;
			frame[41] = flags;
			view.setUint16(42, 65535);
			if (body.byteLength) frame.set(body, 48);
			pseudo[10] = (tcpLen >> 8) & 0xff;
			pseudo[11] = tcpLen & 0xff;
			pseudo.set(frame.subarray(28, 28 + tcpLen), 12);
			view.setUint16(44, checksum(pseudo, 0, 12 + tcpLen));
			return frame;
		};

		const matchIncoming = (ipPacket) => {
			if (ipPacket.byteLength < 40 || ipPacket[9] !== 6) return null;
			const ihl = (ipPacket[0] & 0x0f) * 4;
			if (ipPacket.byteLength < ihl + 20) return null;
			if (u16(ipPacket, ihl) !== targetPort) return null;
			if (u16(ipPacket, ihl + 2) !== sourcePort) return null;
			return {
				flags: ipPacket[ihl + 13],
				sequence: u32(ipPacket, ihl + 4),
				payloadOffset: ihl + ((ipPacket[ihl + 12] >> 4) & 0x0f) * 4,
			};
		};

		// SYN
		await withTimeout(writer.write(buildTcpFrame(0x02)), timeout, 'SSTP SYN timeout');
		seq = (seq + 1) >>> 0;
		let tcpReady = false;
		for (let i = 0; i < 30; i++) {
			const pkt = await readPacket();
			if (pkt.isControl) continue;
			const ppp = parsePpp(pkt.body);
			if (!ppp || ppp.protocol !== PPP_IPV4) continue;
			const tcp = matchIncoming(ppp.ipPacket);
			if (!tcp || (tcp.flags & 0x12) !== 0x12) continue;
			ack = (tcp.sequence + 1) >>> 0;
			writer.write(buildTcpFrame(0x10)).catch(() => { });
			tcpReady = true;
			break;
		}
		if (!tcpReady) throw new Error('SSTP TCP handshake to target timed out');
		onLog(`SSTP 通道就绪 ${sourceIp} -> ${targetIp}:${targetPort}`);

		let closedResolve, closedReject;
		const closed = new Promise((resolve, reject) => {
			closedResolve = resolve;
			closedReject = reject;
		});
		closed.catch(() => { });
		let settled = false;
		const settle = (fn, v) => { if (settled) return; settled = true; fn(v); };

		let ctrl = null;
		const readable = new ReadableStream({
			start(c) { ctrl = c; },
			cancel() { close(); settle(closedResolve); },
		});

		(async () => {
			try {
				let pending = [], pendingLen = 0;
				const flush = () => {
					if (!pendingLen || !ctrl) return;
					ctrl.enqueue(pending.length === 1 ? pending[0] : concat(...pending));
					pending = [];
					pendingLen = 0;
					writer.write(buildTcpFrame(0x10)).catch(() => { });
				};
				for (; ;) {
					const pkt = await readPacket(120000);
					if (pkt.isControl) continue;
					const ppp = parsePpp(pkt.body);
					if (!ppp || ppp.protocol !== PPP_IPV4) continue;
					const inc = matchIncoming(ppp.ipPacket);
					if (!inc) continue;

					if (inc.payloadOffset < ppp.ipPacket.byteLength) {
						const payload = ppp.ipPacket.subarray(inc.payloadOffset);
						if (payload.byteLength) {
							ack = (inc.sequence + payload.byteLength) >>> 0;
							pending.push(new Uint8Array(payload));
							pendingLen += payload.byteLength;
						}
					}
					if (inc.flags & 0x01) { // FIN
						flush();
						ack = (ack + 1) >>> 0;
						writer.write(buildTcpFrame(0x11)).catch(() => { });
						try { ctrl?.close(); } catch (e) { }
						close();
						settle(closedResolve);
						return;
					}
					if (buffered.byteLength < 4 || pendingLen >= 32768) flush();
				}
			} catch (err) {
				try { ctrl?.error(err); } catch (e) { }
				settle(closedReject, err);
				close();
			}
		})();

		const writable = new WritableStream({
			async write(chunk) {
				const bytes = toU8(chunk);
				if (!bytes.byteLength) return;
				if (bytes.byteLength <= MSS) {
					await writer.write(buildTcpFrame(0x18, bytes));
					seq = (seq + bytes.byteLength) >>> 0;
					return;
				}
				const frames = [];
				for (let off = 0; off < bytes.byteLength; off += MSS) {
					const seg = bytes.subarray(off, Math.min(off + MSS, bytes.byteLength));
					frames.push(buildTcpFrame(0x18, seg));
					seq = (seq + seg.byteLength) >>> 0;
				}
				await writer.write(concat(...frames));
			},
			close() {
				return writer.write(buildTcpFrame(0x11)).catch(() => { });
			},
			abort(err) {
				close();
				if (err) settle(closedReject, err);
			},
		});

		return { readable, writable, closed, close, sourceIp, targetIp };
	} catch (err) {
		close();
		throw err;
	}
}

/**
 * 通过 SSTP 隧道向目标发一个 HTTP 请求，收集响应（用于可用性检测 + 出口识别）。
 */
export async function sstpProbe({
	host, port = 443, dial, resolve,
	targetHost = 'ip-api.com', targetPort = 80,
	path = '/json/?fields=status,country,countryCode,city,isp,org,as,query',
	hostHeader = null,
	timeout = DEFAULT_TIMEOUT,
	maxBytes = 4096,
}) {
	const started = Date.now();
	const conn = await sstpConnect({
		host, port, username: 'vpn', password: 'vpn',
		targetHost, targetPort, dial, resolve, timeout,
	});
	try {
		const w = conn.writable.getWriter();
		const req = `GET ${path} HTTP/1.1\r\nHost: ${hostHeader || targetHost}\r\n`
			+ 'User-Agent: Mozilla/5.0 (sstp-probe)\r\nAccept: */*\r\nConnection: close\r\n\r\n';
		await withTimeout(w.write(new TextEncoder().encode(req)), timeout, 'write timeout');
		w.releaseLock();

		const r = conn.readable.getReader();
		const chunks = [];
		let total = 0;
		const deadline = Date.now() + timeout;
		for (; ;) {
			const left = deadline - Date.now();
			if (left <= 0) break;
			const { value, done } = await Promise.race([
				r.read(),
				new Promise((_, rej) => setTimeout(() => rej(new Error('read timeout')), left)),
			]);
			if (done) break;
			if (value?.byteLength) {
				chunks.push(new Uint8Array(value));
				total += value.byteLength;
				if (total >= maxBytes) break;
			}
		}
		try { r.cancel(); } catch (e) { }

		const buf = concat(...chunks);
		const text = new TextDecoder('utf-8', { fatal: false }).decode(buf);
		const statusLine = text.split('\r\n')[0] || '';
		const httpOk = /HTTP\/1\.[01]\s+2\d\d/.test(statusLine);
		const bodyStart = text.indexOf('\r\n\r\n');
		const body = bodyStart >= 0 ? text.slice(bodyStart + 4) : '';

		return {
			success: total > 0 && (httpOk || statusLine.startsWith('HTTP/')),
			bytes: total,
			status_line: statusLine,
			body,
			latency_ms: Date.now() - started,
			assigned_ip: conn.sourceIp,
			target_ip: conn.targetIp,
		};
	} finally {
		try { conn.close(); } catch (e) { }
	}
}

export async function sstpCheck(opts) {
	const r = await sstpProbe(opts);
	return { success: r.success, bytes: r.bytes, banner: r.status_line, latency_ms: r.latency_ms };
}
