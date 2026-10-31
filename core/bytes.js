// 字节工具：Workers / Node 通用（不依赖 Buffer）

export const EMPTY = new Uint8Array(0);

export function toU8(data) {
	if (data instanceof Uint8Array) return data;
	if (data instanceof ArrayBuffer) return new Uint8Array(data);
	if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
	if (typeof data === 'string') return new TextEncoder().encode(data);
	return new Uint8Array(data || 0);
}

export function concat(...chunks) {
	const list = chunks.map(toU8);
	const total = list.reduce((n, c) => n + c.byteLength, 0);
	const out = new Uint8Array(total);
	let off = 0;
	for (const c of list) { out.set(c, off); off += c.byteLength; }
	return out;
}

export const u16 = (b, o = 0) => (b[o] << 8) | b[o + 1];
export const u32 = (b, o = 0) => ((b[o] << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3]) >>> 0;
export const randU16 = () => u16(crypto.getRandomValues(new Uint8Array(2)));
export const randU32 = () => u32(crypto.getRandomValues(new Uint8Array(4)));

export function checksum(bytes, offset, length) {
	let sum = 0;
	for (let i = offset; i < offset + length - 1; i += 2) sum += u16(bytes, i);
	if (length & 1) sum += bytes[offset + length - 1] << 8;
	while (sum >> 16) sum = (sum & 0xffff) + (sum >> 16);
	return (~sum) & 0xffff;
}

export async function withTimeout(promise, ms, message) {
	let timer;
	try {
		return await Promise.race([
			promise,
			new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(message || 'timeout')), ms); }),
		]);
	} finally {
		clearTimeout(timer);
	}
}

export function isIPv4(value) {
	const parts = String(value || '').split('.');
	return parts.length === 4 && parts.every(p => /^\d{1,3}$/.test(p) && Number(p) <= 255);
}

export function stripBrackets(host = '') {
	const h = String(host || '').trim();
	return h.startsWith('[') && h.endsWith(']') ? h.slice(1, -1) : h;
}

export function hex(bytes) {
	let s = '';
	for (const b of bytes) s += b.toString(16).padStart(2, '0');
	return s;
}

export function uuidToHex(uuid) {
	return String(uuid).replace(/-/g, '').toLowerCase();
}

export function ipv6ToString(bytes) {
	const words = [];
	for (let i = 0; i < 16; i += 2) words.push(((bytes[i] << 8) | bytes[i + 1]).toString(16));
	// 压缩连续 0
	let best = -1, bestLen = 0, cur = -1, curLen = 0;
	for (let i = 0; i < 8; i++) {
		if (words[i] === '0') { if (cur < 0) { cur = i; curLen = 0; } curLen++; if (curLen > bestLen) { best = cur; bestLen = curLen; } }
		else { cur = -1; curLen = 0; }
	}
	if (bestLen < 2) return words.join(':');
	return words.slice(0, best).join(':') + '::' + words.slice(best + bestLen).join(':');
}

export function sleep(ms) {
	return new Promise(r => setTimeout(r, ms));
}
