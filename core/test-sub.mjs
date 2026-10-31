// 验证订阅输出：mode=auto 选优 / cf=host CF 优选域名 / sni/host 指向真实域名
import { routeHttp, readConfig } from './router.js';
import fs from 'node:fs/promises';

let pass = 0, fail = 0;
const ok = (cond, msg) => { if (cond) { pass++; console.log('  ✓', msg); } else { fail++; console.log('  ✗', msg); } };

const env = {
	UUID: 'b2f9a3c4-1d2e-4f5a-8b9c-0d1e2f3a4b5c',
	ADMIN: 't',
	CHAIN: 'always',
	__readFile: (n) => fs.readFile(new URL(`../public/${n}`, import.meta.url), 'utf8'),
};
const cfg = readConfig(env);
const hostname = 'edt.example.com';

const run = async (q) => routeHttp({
	pathname: '/sub', query: new URLSearchParams(q), origin: `https://${hostname}`,
	hostname, env, dial: () => {}, resolve: async () => '', cf: null,
});

const body = (r) => r.body;
const asLinks = (text) => text.trim().split('\n').map(l => new URL(l.replace(/^vless:\/\//, 'http://')));

const decodedPath = (line) => decodeURIComponent(new URL(line.replace(/^vless:\/\//, 'http://')).searchParams.get('path') || '');

let r = await run('token=t&format=text&mode=auto');
let lines = body(r).trim().split('\n');
ok(lines.length === 1, 'mode=auto 输出恰好一条');
ok(/node=n[0-9a-f]+/.test(decodedPath(lines[0])), 'auto 链接带 ?node=<id>');
ok(!lines[0].includes('sni=undefined'), 'auto 链接 sni 正常');

r = await run('token=t&format=text&mode=auto&cf=host');
lines = body(r).trim().split('\n');
const links = asLinks(body(r));
ok(links.every(l => l.hostname !== hostname), 'CF 优选模式下 connect host 不是真实域名');
ok(links.every(l => l.searchParams.get('sni') === hostname), 'CF 优选时 sni/host 都指向真实域名');
ok(new Set(links.map(l => decodeURIComponent(l.hash.slice(1)))).size === links.length, 'CF 优选下每个链接标签唯一');

// multi + cf=ip：地址数量应受 cf_limit 控制，而不是写死 1
r = await run('token=t&format=text&mode=multi&cf=ip&cf_limit=2&limit=1');
const multiCf = body(r).trim().split('\n').filter(Boolean);
if (r.headers['x-cf-ip-fallback'] !== '1') {
	ok(multiCf.length === 2, 'multi+cf=ip 时 cf_limit 生效（limit=1 节点 × 2 IP = 2 条）', String(multiCf.length));
} else {
	console.log('      （CFHub 不可达，跳过 multi+cf_limit 断言）');
}

r = await run('token=t&format=text&limit=2');
lines = body(r).trim().split('\n');
ok(lines.length === 2, 'limit=N 限制输出条数');
ok(lines.every(l => l.includes('sni=' + hostname)), '多节点模式下 sni 也指向真实域名');

// cf=host 用内置域名组；cf=ip 用 CFHub（可能离线）
r = await run('token=t&format=text&mode=auto&cf=host');
ok(r.status === 200, 'cf=host 不报错');
ok(/sni=edt\.example\.com/.test(body(r)), 'cf=host 链接 sni 还是真实域名');

// cf=ip 依赖外部 CFHub；拿不到时回落真实域名，且带 x-cf-ip-fallback 头
r = await run('token=t&format=text&mode=auto&cf=ip&cf_limit=2');
ok(r.status === 200, 'cf=ip 即使 CFHub 不可达也不报错');
ok(body(r).trim().length > 0, 'cf=ip 至少输出一条链接');
if (r.headers['x-cf-ip-fallback'] === '1') {
	console.log('      （CFHub 不可达，已回退真实域名）');
	ok(body(r).includes('sni=' + hostname), '回退时 sni 仍是真实域名');
} else {
	console.log('      （CFHub 正常返回 IP）');
}

r = await run('token=wrong&format=text');
ok(r.status === 403, '错误 token 返回 403');

r = await run('token=t&mode=single&format=text');
lines = body(r).trim().split('\n');
ok(lines.length === 1, 'mode=single 输出一条');
ok(lines[0].includes('sni=edt.example.com'), 'single 模式 sni 也正常');
ok(!/node=n/.test(decodedPath(lines[0])), 'single 模式不带 node，后端自动轮换');

console.log(`\n${pass} 通过，${fail} 失败`);
process.exit(fail ? 1 : 0);
