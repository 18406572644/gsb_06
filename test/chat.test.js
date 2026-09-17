'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const { createChatServer } = require('../src/server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 启动一个隔离的测试服务器（内存库、随机端口、默认关闭重发以免干扰计数） */
async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000, // 默认不在测试内重发；重发场景单独配置
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

/** 测试客户端：手动 ACK（测试可控）。log 全量记录供断言；waitFor 消费式匹配（每帧至多满足一个等待者） */
class Client {
  static async connect(port, token) {
    const c = new Client();
    c.log = []; // 全部帧（断言用）
    c.pending = []; // 未被 waitFor 消费的帧
    c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
      for (const w of [...c.waiters]) {
        if (w.pred(m)) {
          c.waiters.splice(c.waiters.indexOf(w), 1);
          clearTimeout(w.timer);
          w.resolve(m);
          return;
        }
      }
      c.pending.push(m);
    });
    c.ws.on('close', () => c._onClosed());
    await new Promise((res, rej) => {
      c.ws.once('open', res);
      c.ws.once('error', rej);
    });
    await c.waitFor((m) => m.type === 'welcome');
    return c;
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  waitFor(pred, timeout = 3000) {
    const idx = this.pending.findIndex(pred);
    if (idx >= 0) {
      const [m] = this.pending.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => {
        reject(new Error('waitFor: timed out'));
      }, timeout);
      this.waiters.push(w);
    });
  }

  /** 已收到的某房间消息帧（seq 列表） */
  roomSeqs(roomId) {
    return this.log.filter((m) => m.type === 'msg' && m.roomId === roomId).map((m) => m.seq);
  }

  close() {
    this.ws.close();
    return this.closed;
  }
}

async function createRoom(client, name) {
  client.send({ type: 'create_room', name });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined.roomId;
}

async function joinRoom(client, room, lastSeq = 0) {
  client.send({ type: 'join', room, lastSeq });
  return client.waitFor((m) => m.type === 'joined');
}

// ---------------------------------------------------------------- 测试用例

test('登录、连接、建房后成为管理员', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    assert.ok(u.userId && u.token);
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    const joined = a.log.find((m) => m.type === 'joined');
    assert.equal(joined.role, 'admin');
    assert.ok(roomId);
    await a.close();
  } finally {
    server.stop();
  }
});

test('发送收到 ACK，房间内广播按 seq 全序投递', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    for (let i = 1; i <= 3; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i}`, content: `hello ${i}` });
    }
    // 发送者收到 3 个 ACK，seq 递增
    for (let i = 1; i <= 3; i++) {
      const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === `m${i}`);
      assert.equal(ack.seq, i);
    }
    // 接收者按序收到 1,2,3
    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 3);
    assert.deepEqual(b.roomSeqs(roomId), [1, 2, 3]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('重复 clientMsgId 幂等：返回同一 seq，不重复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack1 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'dup-1');
    // 网络重试：同 clientMsgId 重发
    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack2 = await a.waitFor(
      (m) => m.type === 'ack' && m.clientMsgId === 'dup-1' && m !== ack1
    );
    assert.equal(ack1.seq, ack2.seq);

    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId);
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [1], '接收端只应收到一次广播');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('断线补发：重连后按序补齐离线期间的消息，且不重复', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'online' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close(); // —— B 掉线 ——

    for (const [i, c] of [2, 3, 4].entries()) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i + 2}`, content: `offline ${c}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm4');

    // —— B 重连，携带本地进度 lastSeq=1 ——
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1);
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2, 3, 4], '补发且仅补发缺口，按序到达');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('已追平的连接重连后不再收到旧消息', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close();

    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1); // 已追平
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [], '不应有任何补发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('服务端对未 ACK 消息重发，ACK 后停止', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 50,
    ackResendAfterMs: 100,
    ackMaxResend: 10,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    // 不 ACK，等服务端重发
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1, 2000);
    assert.ok(b.roomSeqs(roomId).length >= 2, '应观察到至少一次重发');

    b.send({ type: 'ack', roomId, seq: 1 });
    await sleep(100);
    const countAfterAck = b.roomSeqs(roomId).length;
    await sleep(400);
    assert.equal(b.roomSeqs(roomId).length, countAfterAck, 'ACK 后不应再有重发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('禁言：管理员可禁言/解禁，被禁言者发送被拒', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'mute', roomId, userId: ub.userId, minutes: 10 });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'muted' && m.userId === ub.userId);

    b.send({ type: 'msg', roomId, clientMsgId: 'x1', content: 'am i muted?' });
    const err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MUTED');

    a.send({ type: 'unmute', roomId, userId: ub.userId });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'unmuted');

    b.send({ type: 'msg', roomId, clientMsgId: 'x2', content: 'free again' });
    const ack = await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'x2');
    assert.equal(ack.seq, 1);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('权限：普通成员不能禁言他人，管理员不可被禁言', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await joinRoom(c, roomId);

    b.send({ type: 'mute', roomId, userId: uc.userId, minutes: 5 });
    const err1 = await b.waitFor((m) => m.type === 'error');
    assert.equal(err1.code, 'FORBIDDEN');

    a.send({ type: 'mute', roomId, userId: ua.userId, minutes: 5 });
    const err2 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('连接数限制：单用户连接数超限被拒绝', async () => {
  const { server, port } = await startServer({ maxConnectionsPerUser: 2 });
  try {
    const u = await login(port, 'alice');
    const c1 = await Client.connect(port, u.token);
    const c2 = await Client.connect(port, u.token);
    await assert.rejects(
      Client.connect(port, u.token),
      /503|TOO_MANY_DEVICES|Unexpected server response/
    );
    await c1.close();
    await c2.close();
  } finally {
    server.stop();
  }
});

test('发送限流：突发超过令牌桶被拒绝', async () => {
  const { server, port } = await startServer({ rateLimitPerSec: 1, rateLimitBurst: 2 });
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');

    for (let i = 0; i < 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `r${i}`, content: `spam ${i}` });
    }
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'RATE_LIMITED');
    assert.ok(err);
    await sleep(300);
    const ackCount = a.log.filter((m) => m.type === 'ack').length;
    assert.equal(ackCount, 2, '突发容量为 2，其余应被限流');
    await a.close();
  } finally {
    server.stop();
  }
});

test('历史消息分页拉取', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    for (let i = 1; i <= 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `h${i}`, content: `msg ${i}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'h5');

    a.send({ type: 'history', roomId, beforeSeq: 4, limit: 2 });
    const h = await a.waitFor((m) => m.type === 'history');
    assert.deepEqual(h.messages.map((m) => m.seq), [2, 3], '升序返回 beforeSeq 之前的一页');
    assert.equal(h.hasMore, true);
    await a.close();
  } finally {
    server.stop();
  }
});

test('服务端游标兜底：新设备不带 lastSeq 时从已确认进度继续', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'first' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    b.send({ type: 'ack', roomId, seq: 1 }); // 上报确认进度
    await sleep(100);
    await b.close();

    a.send({ type: 'msg', roomId, clientMsgId: 'm2', content: 'second' });
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm2');

    // 新设备重连，不带 lastSeq —— 应使用服务端游标，只补 seq 2
    b = await Client.connect(port, ub.token);
    b.send({ type: 'join', room: roomId });
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('持久化：服务重启后消息不丢失', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-test-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    let token, roomId;
    {
      const { server, port } = await startServer({ dbPath });
      const u = await login(port, 'alice');
      token = u.token;
      const a = await Client.connect(port, token);
      roomId = await createRoom(a, 'persist');
      for (let i = 1; i <= 3; i++) {
        a.send({ type: 'msg', roomId, clientMsgId: `p${i}`, content: `durable ${i}` });
      }
      await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'p3');
      await a.close();
      server.stop();
    }
    {
      const { server, port } = await startServer({ dbPath });
      const a = await Client.connect(port, token); // 同一 token 仍有效
      await joinRoom(a, roomId, 0);
      await a.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
      assert.deepEqual(a.roomSeqs(roomId), [1, 2, 3], '重启后历史消息完整可补发');
      await a.close();
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ------------------------------------------------------------ 权限版本化 / 并发

/** 收集某房间内目标成员的权限通知 */
function permNotices(client, roomId, userId) {
  return client.log.filter(
    (m) => m.type === 'notice' && m.roomId === roomId && m.userId === userId
  );
}

/** 建房并把 bob 提升为第二名管理员，返回 {roomId}；a/b/c 均已在房内 */
async function setupTwoAdmins(port, a, b, c, roomName) {
  const ua = await login(port, 'alice');
  const ub = await login(port, 'bob');
  const uc = await login(port, 'carol');
  const [a2, b2, c2] = await Promise.all([
    Client.connect(port, ua.token),
    Client.connect(port, ub.token),
    Client.connect(port, uc.token),
  ]);
  Object.assign(a, { c: a2, userId: ua.userId });
  Object.assign(b, { c: b2, userId: ub.userId });
  Object.assign(c, { c: c2, userId: uc.userId });
  const roomId = await createRoom(a2, roomName);
  await joinRoom(b2, roomId);
  await joinRoom(c2, roomId);
  a2.send({ type: 'set_role', roomId, userId: ub.userId, role: 'admin', version: 0 });
  await b2.waitFor((m) => m.type === 'notice' && m.event === 'role' && m.userId === ub.userId);
  return { roomId, ua, ub, uc };
}

test('通知帧携带权威状态与版本号：version/rev/mutedUntil 一致', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'mute', roomId, userId: ub.userId, minutes: 10 });
    const n = await b.waitFor((m) => m.type === 'notice' && m.event === 'muted');
    assert.equal(n.version, 1);
    assert.ok(Number.isInteger(n.rev) && n.rev >= 1);
    assert.equal(n.mutedUntil, n.until);
    assert.equal(n.role, 'member');
    assert.equal(n.by, ua.userId);

    // 数据库终态与通知帧同源
    const row = server.db.getMember(roomId, ub.userId);
    assert.equal(row.permVersion, 1);
    assert.equal(row.mutedUntil, n.mutedUntil);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('并发：两个管理员同时禁言/解禁，数据库终态、发送判定与最终通知一致', async () => {
  const { server, port } = await startServer();
  try {
    const a = {}, b = {}, c = {};
    const { roomId, ua, ub, uc } = await setupTwoAdmins(port, a, b, c, 'general');
    const [ac, bc, cc] = [a.c, b.c, c.c];

    // 两帧背靠背发出（不带 version，模拟两个管理员各按各的界面几乎同时点击）
    ac.send({ type: 'mute', roomId, userId: uc.userId, minutes: 10 });
    bc.send({ type: 'unmute', roomId, userId: uc.userId });

    // 等到两条通知都到达
    await cc.waitFor((m) => m.type === 'notice' && m.userId === uc.userId && m.version === 2);
    await sleep(100);
    const notices = permNotices(cc, roomId, uc.userId);
    assert.equal(notices.length, 2, '恰好两条通知');
    assert.deepEqual(notices.map((n) => n.version), [1, 2], '版本号连续递增');
    assert.ok(notices[1].rev > notices[0].rev, 'rev 全局递增给出可追溯顺序');

    // —— 最终通知（version 最大者）即数据库终态 ——
    const winner = notices[1];
    const row = server.db.getMember(roomId, uc.userId);
    assert.equal(row.permVersion, 2);
    assert.equal(row.mutedUntil, winner.mutedUntil, 'DB 终态必须等于最终通知');

    // —— 消息发送判定与终态一致 ——
    cc.send({ type: 'msg', roomId, clientMsgId: 'race-1', content: 'after race' });
    if (winner.event === 'unmuted') {
      const ack = await cc.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'race-1');
      assert.ok(ack.seq >= 1, '最终状态为解禁：发送成功');
    } else {
      const err = await cc.waitFor((m) => m.type === 'error' && m.code === 'MUTED');
      assert.equal(err.details.version, 2, '最终状态为禁言：按权威版本拒绝');
    }
    await Promise.all([ac.close(), bc.close(), cc.close()]);
  } finally {
    server.stop();
  }
});

test('乐观版本号：持有过期视图的操作被拒绝（无写入、无通知），可带新版本重试', async () => {
  const { server, port } = await startServer();
  try {
    const a = {}, b = {}, c = {};
    const { roomId, uc } = await setupTwoAdmins(port, a, b, c, 'general');
    const [ac, , cc] = [a.c, b.c, c.c];

    // 管理员 A 先禁言成功：carol 的版本变为 1
    ac.send({ type: 'mute', roomId, userId: uc.userId, minutes: 10 });
    await cc.waitFor((m) => m.type === 'notice' && m.version === 1);

    // 管理员 B（界面还停留在 version=0）尝试解禁 → 冲突，错误帧回带权威当前状态
    b.c.send({ type: 'unmute', roomId, userId: uc.userId, version: 0 });
    const err = await b.c.waitFor(
      (m) => m.type === 'error' && m.code === 'PERM_VERSION_CONFLICT'
    );
    assert.equal(err.details.current.version, 1);
    assert.equal(err.details.current.mutedUntil, server.db.getMember(roomId, uc.userId).mutedUntil);

    await sleep(150);
    assert.equal(permNotices(cc, roomId, uc.userId).length, 1, '冲突操作不产生通知');
    assert.equal(server.db.getMember(roomId, uc.userId).permVersion, 1, '冲突操作不写库');

    // B 刷新后带正确版本重试 → 成功，版本推进到 2
    b.c.send({ type: 'unmute', roomId, userId: uc.userId, version: 1 });
    await cc.waitFor((m) => m.type === 'notice' && m.version === 2 && m.event === 'unmuted');
    assert.equal(server.db.getMember(roomId, uc.userId).mutedUntil, 0);

    cc.send({ type: 'msg', roomId, clientMsgId: 'retry-1', content: 'works now' });
    const ack = await cc.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'retry-1');
    assert.ok(ack.seq >= 1);
    await Promise.all([a.c.close(), b.c.close(), cc.close()]);
  } finally {
    server.stop();
  }
});

test('审计链：每个成员版本号不重不缺，rev 全局全序，操作人与动作可追溯', async () => {
  const { server, port } = await startServer();
  try {
    const a = {}, b = {}, c = {};
    const { roomId, ua, ub, uc } = await setupTwoAdmins(port, a, b, c, 'general');
    const [ac, bc, cc] = [a.c, b.c, c.c];

    ac.send({ type: 'mute', roomId, userId: uc.userId, minutes: 10 });
    bc.send({ type: 'unmute', roomId, userId: uc.userId });
    ac.send({ type: 'mute', roomId, userId: uc.userId, minutes: 5 });
    bc.send({ type: 'set_role', roomId, userId: uc.userId, role: 'member' }); // 已是 member，无变更
    await cc.waitFor((m) => m.type === 'notice' && m.userId === uc.userId && m.version === 3);
    await sleep(100);

    const events = server.db.getPermEventsAfter(roomId, 0, 100);
    // carol: 两笔禁言 + 一笔解禁（不同连接的到达顺序不固定，只校验集合与版本链）；
    // bob 的提升一笔（version 1）
    const carol = events.filter((e) => e.userId === uc.userId);
    assert.deepEqual(carol.map((e) => e.version), [1, 2, 3], '版本链无缺口');
    assert.deepEqual(
      carol.map((e) => e.action).sort(),
      ['mute', 'mute', 'unmute']
    );
    assert.deepEqual(
      carol.map((e) => e.actorId).sort(),
      [ua.userId, ua.userId, ub.userId].sort(),
      '每笔变更都记录了实际操作人'
    );
    const revs = events.map((e) => e.rev);
    assert.deepEqual(revs, [...revs].sort((x, y) => x - y), 'rev 严格有序');
    assert.equal(new Set(revs).size, revs.length, 'rev 不重复');

    // 无变更的操作（重复授予相同角色）不产生事件
    assert.equal(events.filter((e) => e.userId === uc.userId && e.action === 'role').length, 0);
    await Promise.all([ac.close(), bc.close(), cc.close()]);
  } finally {
    server.stop();
  }
});

test('set_role：成员无权操作；管理员不能降自己；升降权产生版本化通知', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const [a, b, c] = await Promise.all([
      Client.connect(port, ua.token),
      Client.connect(port, ub.token),
      Client.connect(port, uc.token),
    ]);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await joinRoom(c, roomId);

    // 普通成员不能改角色
    b.send({ type: 'set_role', roomId, userId: uc.userId, role: 'admin' });
    const err1 = await b.waitFor((m) => m.type === 'error' && m.code === 'FORBIDDEN');
    assert.ok(err1);

    // 管理员不能降自己（房间必须至少保留一名管理员）
    a.send({ type: 'set_role', roomId, userId: ua.userId, role: 'member' });
    const err2 = await a.waitFor((m) => m.type === 'error' && m.code === 'FORBIDDEN');
    assert.ok(err2);

    // 提升 carol：全员收到 role 通知，版本号为 1，DB 角色更新
    a.send({ type: 'set_role', roomId, userId: uc.userId, role: 'admin' });
    const n = await c.waitFor((m) => m.type === 'notice' && m.event === 'role' && m.userId === uc.userId);
    assert.equal(n.role, 'admin');
    assert.equal(n.version, 1);
    assert.equal(server.db.getMember(roomId, uc.userId).role, 'admin');

    // 降回 member：版本推进到 2
    a.send({ type: 'set_role', roomId, userId: uc.userId, role: 'member' });
    await c.waitFor(
      (m) => m.type === 'notice' && m.event === 'role' && m.userId === uc.userId && m.version === 2
    );
    assert.equal(server.db.getMember(roomId, uc.userId).role, 'member');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('幂等无变更：对未禁言成员解禁不产生新版本与通知', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'unmute', roomId, userId: ub.userId });
    await sleep(200);
    assert.equal(permNotices(b, roomId, ub.userId).length, 0);
    assert.equal(server.db.getMember(roomId, ub.userId).permVersion, 0);
    assert.equal(server.db.getPermEventsAfter(roomId, 0, 100).length, 0);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('禁言已过期：DB 层写入过去的截止时间后，消息发送不被阻止', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    // 直接构造一条「已过期」的禁言记录（不经过 minutes 校验）
    server.db.changeMemberPermission({
      roomId, actorId: ua.userId, targetId: ub.userId,
      action: 'mute', mutedUntil: Date.now() - 1,
    });
    assert.equal(server.db.getMember(roomId, ub.userId).mutedUntil < Date.now(), true);

    b.send({ type: 'msg', roomId, clientMsgId: 'exp-1', content: 'mute already expired' });
    const ack = await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'exp-1');
    assert.equal(ack.seq, 1, '过期禁言不得继续阻止发送');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('重连快照：joined 帧下发的 permVersion/mutedUntil 与数据库一致', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'mute', roomId, userId: ub.userId, minutes: 10 });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'muted');
    await b.close();

    b = await Client.connect(port, ub.token);
    const joined = await joinRoom(b, roomId, 0);
    const row = server.db.getMember(roomId, ub.userId);
    assert.equal(joined.permVersion, row.permVersion);
    assert.equal(joined.mutedUntil, row.mutedUntil);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});
