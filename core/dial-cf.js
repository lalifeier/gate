// Cloudflare Workers 侧 TCP 拨号器（包一层 workers sockets）
import { connect } from 'cloudflare:sockets';

export function createCfDial({ connectTimeoutMs = 12000 } = {}) {
	return function dial(host, port, { secure = false, serverName = null } = {}) {
		const hostname = String(host).replace(/^\[|\]$/g, '');
		const opts = { allowHalfOpen: false };
		if (secure) {
			opts.secureTransport = 'on';
			opts.servername = serverName || hostname;
		}
		const socket = connect({ hostname, port: Number(port) }, opts);
		const opened = Promise.race([
			socket.opened,
			new Promise((_, rej) => setTimeout(() => rej(new Error(`connect ${hostname}:${port} timeout`)), connectTimeoutMs)),
		]);
		return {
			opened,
			readable: socket.readable,
			writable: socket.writable,
			closed: socket.closed,
			close() { try { socket.close(); } catch (e) { } },
		};
	};
}

export async function resolveA(host) {
	const res = await fetch(`https://1.1.1.1/dns-query?name=${encodeURIComponent(host)}&type=A`, {
		headers: { accept: 'application/dns-json' },
	});
	const json = await res.json();
	const a = (json.Answer || []).find(x => x.type === 1);
	if (!a) throw new Error(`DoH 未解析到 IPv4: ${host}`);
	return a.data;
}
