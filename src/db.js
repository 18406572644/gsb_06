'use strict';

const { DatabaseSync } = require('node:sqlite');
const { now } = require('./util');

/**
 * 持久层：SQLite（WAL 模式）。
 *
 * 可靠性设计要点：
 * 1. 消息与房间序号 seq 在同一事务中「先落库、后广播」——进程崩溃也不丢已确认消息。
 * 2. messages 上 (room_id, sender_id, client_msg_id) 唯一约束——客户端重试/网络重复
 *    提交同一条消息时不会产生重复记录，实现发送幂等。
 * 3. seq 为每房间单调递增序号，由 rooms.last_seq 计数器在事务内分配——保证房间内
 *    消息全序（时序可控），客户端可凭 seq 检测空洞并触发补发。
 * 4. 成员权限（角色/禁言）带 perm_version 乐观版本号；任何变更都在单条 IMMEDIATE
 *    事务内完成「读当前行 → 权限守卫 → 版本校验 → 写新状态(version+1) → 追加审计
 *    事件」。事务提交后才用权威行构造通知，保证数据库终态、发送时权限判定、在线
 *    通知三者基于同一结果；member_perm_events.rev 为全局自增序号，给出跨管理员
 *    操作的可追溯全序，(room_id,user_id,version) 唯一约束保证版本链不重不缺。
 */

/** 权限层业务错误（区别于 SQLite 驱动错误），携带稳定 code 供协议层直接下发 */
class ChatDBError extends Error {
  constructor(code, message, extra = {}) {
    super(message);
    this.name = 'ChatDBError';
    this.code = code;
    Object.assign(this, extra); // 如 current：版本冲突时回带权威当前状态
  }
}

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS users (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  token_random TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rooms (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  created_by TEXT NOT NULL REFERENCES users(id),
  created_at INTEGER NOT NULL,
  last_seq   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS members (
  room_id        TEXT NOT NULL REFERENCES rooms(id),
  user_id        TEXT NOT NULL REFERENCES users(id),
  role           TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin','member')),
  muted_until    INTEGER NOT NULL DEFAULT 0,
  joined_at      INTEGER NOT NULL,
  perm_version   INTEGER NOT NULL DEFAULT 0, -- 权限版本号：每次角色/禁言变更 +1（乐观并发控制）
  perm_updated_at INTEGER NOT NULL DEFAULT 0, -- 最近一次权限变更的提交时间戳（毫秒）
  PRIMARY KEY (room_id, user_id)
);

-- 成员权限变更审计：每个成员一条无缺口的版本链，rev 为跨所有成员的全局全序。
-- 通知帧携带 rev/version，客户端可排序、去重、追序；唯一约束兜底防止版本断档/重复。
CREATE TABLE IF NOT EXISTS member_perm_events (
  rev         INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  user_id     TEXT NOT NULL REFERENCES users(id),
  version     INTEGER NOT NULL,
  action      TEXT NOT NULL CHECK (action IN ('mute','unmute','role')),
  role        TEXT NOT NULL,
  muted_until INTEGER NOT NULL,
  actor_id    TEXT NOT NULL REFERENCES users(id),
  ts          INTEGER NOT NULL,
  UNIQUE (room_id, user_id, version)
);
CREATE INDEX IF NOT EXISTS idx_member_perm_events_room
  ON member_perm_events (room_id, rev);

CREATE TABLE IF NOT EXISTS messages (
  room_id       TEXT NOT NULL REFERENCES rooms(id),
  seq           INTEGER NOT NULL,
  client_msg_id TEXT NOT NULL,
  sender_id     TEXT NOT NULL REFERENCES users(id),
  content       TEXT NOT NULL,
  ts            INTEGER NOT NULL,
  PRIMARY KEY (room_id, seq),
  UNIQUE (room_id, sender_id, client_msg_id)  -- 幂等键
);

-- 服务端保存的每用户每房间已确认游标（断线补发的兜底依据）
CREATE TABLE IF NOT EXISTS cursors (
  room_id      TEXT NOT NULL REFERENCES rooms(id),
  user_id      TEXT NOT NULL REFERENCES users(id),
  last_ack_seq INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);
`;

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts
    FROM messages m JOIN users u ON u.id = m.sender_id
`;

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(SCHEMA);
    this._migrate();
    this._prepare();
  }

  /** 旧版本数据库补列（members 表在 perm_version 之前创建的情况） */
  _migrate() {
    const cols = new Set(this.db.prepare('PRAGMA table_info(members)').all().map((c) => c.name));
    if (!cols.has('perm_version')) {
      this.db.exec('ALTER TABLE members ADD COLUMN perm_version INTEGER NOT NULL DEFAULT 0');
    }
    if (!cols.has('perm_updated_at')) {
      this.db.exec('ALTER TABLE members ADD COLUMN perm_updated_at INTEGER NOT NULL DEFAULT 0');
    }
  }

  _prepare() {
    const d = this.db;
    this.stmt = {
      insertUser: d.prepare('INSERT INTO users (id, name, token_random, created_at) VALUES (?, ?, ?, ?)'),
      userByName: d.prepare('SELECT * FROM users WHERE name = ?'),
      userById: d.prepare('SELECT * FROM users WHERE id = ?'),

      insertRoom: d.prepare('INSERT INTO rooms (id, name, created_by, created_at) VALUES (?, ?, ?, ?)'),
      roomById: d.prepare('SELECT * FROM rooms WHERE id = ?'),
      roomByName: d.prepare('SELECT * FROM rooms WHERE name = ?'),
      roomsForUser: d.prepare(
        `SELECT r.id, r.name, r.last_seq AS lastSeq, m.role, m.muted_until AS mutedUntil,
                m.perm_version AS permVersion
           FROM rooms r JOIN members m ON m.room_id = r.id
          WHERE m.user_id = ? ORDER BY r.created_at`
      ),

      upsertMember: d.prepare(
        `INSERT INTO members (room_id, user_id, role, muted_until, joined_at)
         VALUES (?, ?, ?, 0, ?)
         ON CONFLICT (room_id, user_id) DO NOTHING`
      ),
      member: d.prepare('SELECT * FROM members WHERE room_id = ? AND user_id = ?'),
      countAdmins: d.prepare(
        `SELECT COUNT(*) AS n FROM members WHERE room_id = ? AND role = 'admin'`
      ),
      // 乐观条件更新：仅当版本号未变时提交，RETURNING 直接给出提交后的权威行
      updateMemberPerm: d.prepare(
        `UPDATE members
            SET role = ?, muted_until = ?, perm_version = perm_version + 1, perm_updated_at = ?
          WHERE room_id = ? AND user_id = ? AND perm_version = ?
          RETURNING *`
      ),
      insertPermEvent: d.prepare(
        `INSERT INTO member_perm_events
           (room_id, user_id, version, action, role, muted_until, actor_id, ts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING rev`
      ),
      permEventsAfter: d.prepare(
        `SELECT rev, room_id AS roomId, user_id AS userId, version, action,
                role, muted_until AS mutedUntil, actor_id AS actorId, ts
           FROM member_perm_events WHERE room_id = ? AND rev > ? ORDER BY rev LIMIT ?`
      ),
      membersOfRoom: d.prepare(
        `SELECT m.user_id AS userId, u.name, m.role, m.muted_until AS mutedUntil,
                m.perm_version AS permVersion
           FROM members m JOIN users u ON u.id = m.user_id WHERE m.room_id = ?`
      ),

      // —— 消息写入（事务内使用）——
      msgByClientId: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.sender_id = ? AND m.client_msg_id = ?`
      ),
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      insertMsg: d.prepare(
        'INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts) VALUES (?, ?, ?, ?, ?, ?)'
      ),

      // —— 消息读取 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgsBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
      ),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq), updated_at = excluded.updated_at`
      ),
      cursor: d.prepare('SELECT last_ack_seq AS lastAckSeq FROM cursors WHERE room_id = ? AND user_id = ?'),
    };
  }

  /** 在 IMMEDIATE 事务中执行 fn，失败回滚。node:sqlite 为同步驱动，单进程内无并发交错。 */
  _tx(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const r = fn();
      this.db.exec('COMMIT');
      return r;
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* 已回滚 */ }
      throw err;
    }
  }

  // ---------- 用户 ----------

  createUser(id, name, tokenRandom) {
    this.stmt.insertUser.run(id, name, tokenRandom, now());
    return this.stmt.userById.get(id);
  }

  getUserByName(name) { return this.stmt.userByName.get(name); }
  getUserById(id) { return this.stmt.userById.get(id); }

  // ---------- 房间与成员 ----------

  createRoom(id, name, creatorId) {
    return this._tx(() => {
      this.stmt.insertRoom.run(id, name, creatorId, now());
      // 创建者即管理员
      this.stmt.upsertMember.run(id, creatorId, 'admin', now());
      return this.stmt.roomById.get(id);
    });
  }

  getRoom(id) { return this.stmt.roomById.get(id); }
  getRoomByName(name) { return this.stmt.roomByName.get(name); }
  listRoomsForUser(userId) { return this.stmt.roomsForUser.all(userId); }
  listMembers(roomId) { return this.stmt.membersOfRoom.all(roomId); }

  joinRoom(roomId, userId) {
    this.stmt.upsertMember.run(roomId, userId, 'member', now());
    return this.stmt.member.get(roomId, userId);
  }

  getMember(roomId, userId) {
    return this._shapeMember(this.stmt.member.get(roomId, userId));
  }

  /** 成员行 -> 对外形状（snake_case 转 camelCase，权限版本一并暴露） */
  _shapeMember(row) {
    if (!row) return null;
    return {
      roomId: row.room_id,
      userId: row.user_id,
      role: row.role,
      mutedUntil: row.muted_until,
      joinedAt: row.joined_at,
      permVersion: row.perm_version,
      permUpdatedAt: row.perm_updated_at,
    };
  }

  /**
   * 成员权限变更的唯一入口（禁言 / 解禁 / 改角色）。
   *
   * 整个判定与写入在一条 IMMEDIATE 事务内完成，杜绝「先读后写」的并发交错：
   *   1. 读操作者当前角色（不信任连接建立时的身份快照）与目标当前行；
   *   2. 业务守卫：不能禁言/降权管理员、不能降自己、房间至少保留一名管理员；
   *   3. 乐观版本校验：expectedVersion 非空且不等于当前版本 → PERM_VERSION_CONFLICT，
   *      回带 current 权威行，且不写任何状态、不产生通知；
   *   4. 条件 UPDATE（WHERE perm_version = expected）+ version 自增，并追加审计事件；
   *   5. 用 RETURNING 拿到提交后的权威行返回——调用方只能基于它构造 ACK/通知，
   *      因此数据库终态、消息发送判定、在线通知帧永远是同一结果。
   *
   * @returns {{member, rev, changed: boolean}} member 为权威行；
   *   changed=false 表示目标状态与请求一致（如对未禁言者解禁），无写入无通知。
   */
  changeMemberPermission({ roomId, actorId, targetId, action, mutedUntil, role, expectedVersion }) {
    return this._tx(() => {
      const actor = this.stmt.member.get(roomId, actorId);
      if (!actor || actor.role !== 'admin') {
        throw new ChatDBError('FORBIDDEN', 'admin role required');
      }
      const target = this.stmt.member.get(roomId, targetId);
      if (!target) throw new ChatDBError('NOT_MEMBER', 'target is not a member');

      // —— 守卫规则（基于事务内刚读出的当前行）——
      if (action === 'mute' && target.role === 'admin') {
        throw new ChatDBError('FORBIDDEN', 'cannot mute an admin');
      }
      if (action === 'role') {
        if (role !== 'admin' && role !== 'member') {
          throw new ChatDBError('BAD_REQUEST', 'role must be admin or member');
        }
        if (targetId === actorId && role !== 'admin') {
          throw new ChatDBError('FORBIDDEN', 'cannot demote yourself');
        }
        if (target.role === 'admin' && role === 'member') {
          const { n } = this.stmt.countAdmins.get(roomId);
          if (n <= 1) throw new ChatDBError('FORBIDDEN', 'room must keep at least one admin');
        }
      }

      const nextRole = action === 'role' ? role : target.role;
      const nextMutedUntil = action === 'mute'
        ? mutedUntil
        : action === 'unmute'
          ? 0
          : target.muted_until; // role 变更不动禁言状态

      // 幂等：已是目标状态则不产生新版本/新事件（如重复解禁、重复授予相同角色）
      if (nextRole === target.role && nextMutedUntil === target.muted_until) {
        return { member: this._shapeMember(target), changed: false, rev: null };
      }

      // —— 乐观版本校验：客户端/调用方持有过期视图则拒绝整笔操作 ——
      if (Number.isInteger(expectedVersion) && expectedVersion !== target.perm_version) {
        throw new ChatDBError('PERM_VERSION_CONFLICT', 'permission changed by another admin', {
          current: this._shapeMember(target),
        });
      }

      const ts = now();
      const updated = this.stmt.updateMemberPerm.get(
        nextRole, nextMutedUntil, ts, roomId, targetId, target.perm_version
      );
      // 条件更新落空只可能是并发已抢先提交（版本被改）→ 冲突，事务回滚
      if (!updated) {
        throw new ChatDBError(
          'PERM_VERSION_CONFLICT',
          'permission changed by another admin',
          { current: this._shapeMember(this.stmt.member.get(roomId, targetId)) }
        );
      }
      const { rev } = this.stmt.insertPermEvent.get(
        roomId, targetId, updated.perm_version, action,
        updated.role, updated.muted_until, actorId, ts
      );
      return { member: this._shapeMember(updated), changed: true, rev };
    });
  }

  /** 拉取房间内 rev > afterRev 的权限事件（重连后补齐离线期间的权限变更，按 rev 全序） */
  getPermEventsAfter(roomId, afterRev, limit) {
    return this.stmt.permEventsAfter.all(roomId, afterRev, limit);
  }

  // ---------- 消息 ----------

  /**
   * 幂等写入消息。
   * 返回 { message, duplicate }：
   *  - duplicate=false：新消息，已分配 seq 并落库（调用方负责广播）；
   *  - duplicate=true ：同 clientMsgId 的消息已存在，直接返回原消息（调用方只回 ACK，不再广播）。
   */
  insertMessage({ roomId, clientMsgId, senderId, content }) {
    return this._tx(() => {
      const existing = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      if (existing) return { message: existing, duplicate: true };

      const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
      const ts = now();
      this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts);
      const message = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      return { message, duplicate: false };
    });
  }

  /** 断线补发：取 seq > afterSeq 的消息（升序，最多 limit 条） */
  getMessagesAfter(roomId, afterSeq, limit) {
    return this.stmt.msgsAfter.all(roomId, afterSeq, limit);
  }

  /** 历史翻页：取 seq < beforeSeq 的消息，返回时按升序排列 */
  getMessagesBefore(roomId, beforeSeq, limit) {
    return this.stmt.msgsBefore.all(roomId, beforeSeq, limit).reverse();
  }

  // ---------- 游标 ----------

  saveCursor(roomId, userId, lastAckSeq) {
    this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, now());
  }

  getCursor(roomId, userId) {
    const row = this.stmt.cursor.get(roomId, userId);
    return row ? row.lastAckSeq : 0;
  }

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB, ChatDBError };
