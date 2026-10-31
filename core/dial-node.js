// Node.js 侧 TCP/TLS 拨号器：把 node net.Socket 包装成 Workers connect() 的形状
// { opened, readable, writable, closed, close }
//
// 支持经由 HTTP CONNECT 代理出网（本地调试 SSTP 节点必需，因为多数 VPN Gate 节点直连不通）：
//   DIAL_PROXY=http://127.0.0.1:1080
// 也可沿用 https_proxy / http_proxy 环境变量。
//
// 注意：代理只承载“到 SSTP 服务器”的外层 TCP。SSTP 隧道内部到目标主机的 TCP 由服务器侧发出。

import net from 'node:net';
import tls from 'node:tls';
import dns from 'node:dns';
import { Readable, Writable } from 'node:stream';

function parseProxy(raw) {
	if (!raw) return null;
	try {
		const u = new URL(raw);
		return {
			host: u.hostname,
			port: Number(u.port || (u.protocol === 'https:' ? 443 : 80)),
			secure: u.protocol === 'https:',
			auth: u.username
				? 'Basic ' + Buffer.from(`${decodeURIComponent(u.username)}:${decodeURIComponent(u.password)}`).toString('base64')
				: null,
		};
	} catch (e) {
		return null;
	}
}

function proxyEnvValue() {
	return process.env.DIAL_PROXY || process.env.https_proxy || process.env.HTTPS_PROXY
		|| process.env.http_proxy || process.env.HTTP_PROXY || '';
}

function tunnelThroughProxy(proxy, host, port, timeoutMs) {
	return new Promise((resolve, reject) => {
		const raw = proxy.secure
			? tls.connect({ host: proxy.host, port: proxy.port, rejectUnauthorized: false })
			: net.connect({ host: proxy.host, port: proxy.port });

		const timer = setTimeout(() => {
			raw.destroy();
			reject(new Error(`proxy connect ${proxy.host}:${proxy.port} timeout`));
		}, timeoutMs);

		const fail = (err) => { clearTimeout(timer); raw.destroy(); reject(err); };
		raw.once('error', fail);

		const start = () => {
			const lines = [`CONNECT ${host}:${port} HTTP/1.1`, `Host: ${host}:${port}`];
			if (proxy.auth) lines.push(`Proxy-Authorization: ${proxy.auth}`);
			raw.write(lines.join('\r\n') + '\r\n\r\n');

			let buf = Buffer.alloc(0);
			const onData = (chunk) => {
				buf = Buffer.concat([buf, chunk]);
				const idx = buf.indexOf('\r\n\r\n');
				if (idx < 0) {
					if (buf.length > 32768) fail(new Error('proxy response too large'));
					return;
				}
				const head = buf.subarray(0, idx).toString('latin1');
				const status = Number((/^HTTP\/\d(?:\.\d)?\s+(\d{3})/.exec(head) || [])[1]);
				const rest = buf.subarray(idx + 4);
				clearTimeout(timer);
				raw.removeListener('data', onData);
				raw.removeListener('error', fail);
				if (!(status >= 200 && status < 300)) {
					raw.destroy();
					reject(new Error(`proxy CONNECT ${host}:${port} failed: ${status}`));
					return;
				}
				if (rest.length) raw.unshift(rest);
				resolve(raw);
			};
			raw.on('data', onData);
		};

		if (proxy.secure) raw.once('secureConnect', start);
		else raw.once('connect', start);
	});
}

export function createNodeDial({ insecure = true, connectTimeoutMs = 12000, proxyUrl = null } = {}) {
	const proxy = parseProxy(proxyUrl ?? proxyEnvValue());

	async function openTcp(hostname, port) {
		if (!proxy) return net.connect({ host: hostname, port });
		return tunnelThroughProxy(proxy, hostname, port, connectTimeoutMs);
	}

	return function dial(host, port, { secure = false, serverName = null } = {}) {
		const hostname = String(host).replace(/^\[|\]$/g, '');
		const portNum = Number(port);

		let sock = null;
		let settled = false;
		let timer = null;

		const opened = new Promise((resolve, reject) => {
			timer = setTimeout(() => {
				if (settled) return;
				settled = true;
				try { sock?.destroy(); } catch (e) { }
				reject(new Error(`connect ${hostname}:${portNum} timeout`));
			}, connectTimeoutMs);

			openTcp(hostname, portNum).then((base) => {
				if (settled) { try { base.destroy(); } catch (e) { } return; }
				base.setNoDelay?.(true);
				base.setKeepAlive?.(true, 30000);
				if (!secure) {
					sock = base;
					clearTimeout(timer);
					settled = true;
					resolve();
					return;
				}
				const t = tls.connect({ socket: base, servername: serverName || hostname, rejectUnauthorized: !insecure });
				t.once('secureConnect', () => {
					if (settled) return;
					sock = t;
					clearTimeout(timer);
					settled = true;
					resolve();
				});
				t.once('error', (e) => {
					if (settled) return;
					clearTimeout(timer);
					settled = true;
					reject(e);
				});
			}).catch((e) => {
				if (settled) return;
				clearTimeout(timer);
				settled = true;
				reject(e);
			});
		});
		opened.catch(() => { });

		const closed = (async () => {
			await opened.catch(() => { });
			await new Promise((r) => sock ? sock.once('close', r) : r());
		})();

		// opened 完成后 sock 才就绪，因此流的 start / write 内部先 await opened
		const readable = new ReadableStream({
			async start(controller) {
				try {
					await opened;
				} catch (e) {
					controller.error(e);
					return;
				}
				const rs = sock;
				rs.on('data', (chunk) => {
					try { controller.enqueue(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)); } catch (e) { }
				});
				rs.once('end', () => { try { controller.close(); } catch (e) { } });
				rs.once('error', (err) => { try { controller.error(err); } catch (e) { } });
			},
			cancel() { try { sock?.destroy(); } catch (e) { } },
		});

		const writable = new WritableStream({
			async write(chunk) {
				await opened;
				return new Promise((resolve, reject) => {
					if (!sock || sock.destroyed) return reject(new Error('socket closed'));
					sock.write(Buffer.from(chunk), (err) => (err ? reject(err) : resolve()));
				});
			},
			async close() {
				try { await opened; } catch (e) { return; }
				return new Promise((resolve) => {
					if (!sock || sock.destroyed) return resolve();
					try { sock.end(() => resolve()); } catch (e) { resolve(); }
				});
			},
			abort() { try { sock?.destroy(); } catch (e) { } },
		});

		return {
			opened,
			readable,
			writable,
			closed,
			close() { try { sock?.destroy(); } catch (e) { } },
		};
	};
}

export async function resolveA(host) {
	const list = await dns.promises.resolve4(String(host));
	if (!list?.length) throw new Error(`no A record: ${host}`);
	return list[0];
}

export async function dohResolveA(host) {
	const res = await fetch(`https://1.1.1.1/dns-query?name=${encodeURIComponent(host)}&type=A`, {
		headers: { accept: 'application/dns-json' },
	});
	const json = await res.json();
	const answer = (json.Answer || []).find(a => a.type === 1);
	if (!answer) throw new Error(`DoH no A record: ${host}`);
	return answer.data;
}

export const _parseProxy = parseProxy;
