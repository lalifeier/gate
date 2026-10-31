// VLESS 协议头解析 / 响应构造（Worker 与 Node 共用）
//
// 请求：ver(1) uuid(16) addonLen(1) addons(cmd(1) port(2) atyp(1) addr ...) payload...
// 响应：ver(1) addonLen(1) addons

import { toU8, hex, uuidToHex, ipv6ToString } from './bytes.js';

export const ATYP_IPV4 = 1;
export const ATYP_DOMAIN = 2;
export const ATYP_IPV6 = 3;

export function uuidBytes(uuid) {
	const h = uuidToHex(uuid);
	if (h.length !== 32) throw new Error('invalid uuid');
	const out = new Uint8Array(16);
	for (let i = 0; i < 16; i++) out[i] = parseInt(h.slice(i * 2, i * 2 + 2), 16);
	return out;
}

export function bytesEqual(a, b) {
	if (a.byteLength !== b.byteLength) return false;
	let diff = 0;
	for (let i = 0; i < a.byteLength; i++) diff |= a[i] ^ b[i];
	return diff === 0;
}

/**
 * 解析 VLESS 请求头。
 * @returns {{ ok: true, cmd:number, hostname:string, port:number, isUDP:boolean, payload:Uint8Array, addons:Uint8Array }
 *          | { ok:false, error:string, needMore:boolean }}
 */
export function parseVlessRequest(buffer, expectedUuid) {
	const data = toU8(buffer);
	if (data.byteLength < 18) return { ok: false, error: 'need more', needMore: true };

	const version = data[0];
	if (version !== 0) return { ok: false, error: `unsupported vless version ${version}` };

	const got = data.subarray(1, 17);
	if (expectedUuid) {
		const want = uuidBytes(expectedUuid);
		if (!bytesEqual(got, want)) return { ok: false, error: 'invalid user' };
	}

	let p = 17;
	const addonLen = data[p++];
	if (data.byteLength < p + addonLen + 5) return { ok: false, error: 'need more', needMore: true };
	const addons = data.subarray(p, p + addonLen);
	p += addonLen;

	const cmd = data[p++];
	const port = (data[p] << 8) | data[p + 1];
	p += 2;
	const atyp = data[p++];

	let hostname = '';
	if (atyp === ATYP_IPV4) {
		if (data.byteLength < p + 4) return { ok: false, error: 'need more', needMore: true };
		hostname = `${data[p]}.${data[p + 1]}.${data[p + 2]}.${data[p + 3]}`;
		p += 4;
	} else if (atyp === ATYP_DOMAIN) {
		if (data.byteLength < p + 1) return { ok: false, error: 'need more', needMore: true };
		const len = data[p++];
		if (data.byteLength < p + len) return { ok: false, error: 'need more', needMore: true };
		hostname = new TextDecoder().decode(data.subarray(p, p + len));
		p += len;
	} else if (atyp === ATYP_IPV6) {
		if (data.byteLength < p + 16) return { ok: false, error: 'need more', needMore: true };
		hostname = ipv6ToString(data.subarray(p, p + 16));
		p += 16;
	} else {
		return { ok: false, error: `unsupported atyp ${atyp}` };
	}

	return {
		ok: true,
		cmd,
		hostname,
		port,
		isUDP: cmd === 2,
		addons,
		payload: data.subarray(p),
	};
}

export function vlessResponseHeader() {
	return new Uint8Array([0, 0]);
}

export function bytesMarker(b) {
	return `${b.byteLength}B ${hex(b.subarray(0, Math.min(8, b.byteLength)))}`;
}

/** 双向管道：在 WebSocket(二进制) 与 socket(readable/writable) 之间搬运 */
export async function pipeSocketToWs(socket, ws, { onError = () => { } } = {}) {
	let reader;
	try {
		reader = socket.readable.getReader();
	} catch (e) {
		onError(e);
		return;
	}
	try {
		for (; ;) {
			const { value, done } = await reader.read();
			if (done) break;
			if (!value || !value.byteLength) continue;
			if (ws.readyState !== 1 /* OPEN */) break;
			ws.send(value);
		}
	} catch (e) {
		onError(e);
	} finally {
		try { reader.releaseLock(); } catch (e) { }
		try { ws.close(); } catch (e) { }
	}
}

export async function pipeWsToSocket(ws, socket, firstPayload) {
	let writer;
	try {
		writer = socket.writable.getWriter();
		if (firstPayload && firstPayload.byteLength) await writer.write(firstPayload);
	} catch (e) {
		try { ws.close(); } catch (_) { }
		return;
	}
	const onMessage = async (ev) => {
		try {
			const chunk = toU8(ev.data);
			if (chunk.byteLength) await writer.write(chunk);
		} catch (e) {
			try { ws.close(); } catch (_) { }
		}
	};
	ws.addEventListener('message', onMessage);
	const onClose = () => {
		try { writer.close().catch(() => { }); } catch (e) { }
	};
	ws.addEventListener('close', onClose);
	ws.addEventListener('error', onClose);
}
