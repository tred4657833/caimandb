'use strict';

/**
 * CaimanDB driver for Node.js / JavaScript.
 *
 * Talks to CaimanDB's query HTTP server (default port 1555) and,
 * optionally, its admin HTTP server (default port 1556) for login.
 * Zero external dependencies -- uses the built-in `fetch` (Node 18+).
 *
 * Wire protocol (see docs/api/http-api.md in the CaimanDB repo):
 *   POST {queryURL}/query?query=<NQL>&db=<db>   -> {success, result, db}
 *   GET  {queryURL}/health
 *   GET  {queryURL}/status
 *   GET  {queryURL}/watch?db=&block=            -> Server-Sent Events
 *   POST {adminURL}/api/v1/auth/login  {username, password}
 *                                                -> {token, user, role, expires}
 *
 * Auth: either HTTP Basic (username/password) or a JWT Bearer token
 * obtained via login() or supplied directly.
 */

class CaimanDBError extends Error {
  constructor(message, status, body) {
    super(message);
    this.name = 'CaimanDBError';
    this.status = status;
    this.body = body;
  }
}

class CaimanDBClient {
  /**
   * @param {Object} opts
   * @param {string} [opts.queryURL]  Base URL of the query server, e.g. "http://localhost:1555"
   * @param {string} [opts.adminURL]  Base URL of the admin server, e.g. "http://localhost:1556"
   * @param {string} [opts.username]  For Basic Auth or login()
   * @param {string} [opts.password]
   * @param {string} [opts.token]     Pre-obtained JWT, used as Bearer token
   * @param {string} [opts.db]       Default database (defaults to "default")
   */
  constructor(opts = {}) {
    this.queryURL = (opts.queryURL || 'http://localhost:1555').replace(/\/$/, '');
    this.adminURL = (opts.adminURL || 'http://localhost:1556').replace(/\/$/, '');
    this.username = opts.username || null;
    this.password = opts.password || null;
    this.token = opts.token || null;
    this.defaultDB = opts.db || 'default';
  }

  _authHeader() {
    if (this.token) return { Authorization: `Bearer ${this.token}` };
    if (this.username != null) {
      const b64 = Buffer.from(`${this.username}:${this.password || ''}`).toString('base64');
      return { Authorization: `Basic ${b64}` };
    }
    return {};
  }

  /** Logs in against the admin server and stores the JWT for subsequent requests. */
  async login(username, password) {
    const res = await fetch(`${this.adminURL}/api/v1/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new CaimanDBError(body.error || `login failed (${res.status})`, res.status, body);
    }
    this.token = body.token;
    this.username = username;
    return body; // { token, user, role, expires }
  }

  /**
   * Executes a raw NQL command, e.g. 'FIND users WHERE age > 18'.
   * Returns the parsed { success, result, db } response.
   */
  async query(nql, db) {
    const url = new URL(`${this.queryURL}/query`);
    url.searchParams.set('query', nql);
    url.searchParams.set('db', db || this.defaultDB);

    const res = await fetch(url.toString(), {
      method: 'POST',
      headers: { ...this._authHeader() },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || body.success === false) {
      throw new CaimanDBError(body.error || body.result || `query failed (${res.status})`, res.status, body);
    }
    return body;
  }

  // ---- Convenience wrappers over query() -------------------------------

  /** INSERT one document (object) or several (array of objects). */
  async insert(block, docOrDocs, db) {
    const payload = Array.isArray(docOrDocs)
      ? JSON.stringify(docOrDocs)
      : JSON.stringify(docOrDocs);
    return this.query(`INSERT ${block} ${payload}`, db);
  }

  /** GET a single document by id. */
  async get(block, id, db) {
    return this.query(`GET ${block} ${id}`, db);
  }

  /**
   * FIND documents.
   * @param {string} block
   * @param {Object} [opts]
   * @param {string} [opts.where]   Raw WHERE clause, e.g. `age > 18 AND status = "active"`
   * @param {string[]} [opts.select] Field names to project
   * @param {string} [opts.order]   e.g. "age:DESC"
   * @param {number} [opts.limit]
   * @param {number} [opts.offset]
   */
  async find(block, opts = {}, db) {
    let cmd = `FIND ${block}`;
    if (opts.select && opts.select.length) cmd += ` SELECT ${opts.select.join(', ')}`;
    if (opts.where) cmd += ` WHERE ${opts.where}`;
    if (opts.order) cmd += ` ORDER ${opts.order}`;
    if (opts.limit != null) cmd += ` LIMIT ${opts.limit}`;
    if (opts.offset != null) cmd += ` OFFSET ${opts.offset}`;
    return this.query(cmd, db);
  }

  /** SEARCH full-text. */
  async search(block, text, opts = {}, db) {
    let cmd = `SEARCH ${block} ${JSON.stringify(text)}`;
    if (opts.exact) cmd += ' EXACT';
    if (opts.fuzzy) cmd += ' FUZZY';
    if (opts.withScore) cmd += ' WITH SCORE';
    if (opts.withMatches) cmd += ' WITH MATCHES';
    return this.query(cmd, db);
  }

  /** UPDATE documents matching a WHERE clause with a raw SET/INC/PUSH clause. */
  async update(block, where, setClause, db) {
    return this.query(`UPDATE ${block} WHERE ${where} ${setClause}`, db);
  }

  /** DELETE documents matching a WHERE clause. */
  async delete(block, where, db) {
    return this.query(`DELETE ${block} WHERE ${where}`, db);
  }

  /** COUNT documents matching an optional WHERE clause. */
  async count(block, where, db) {
    return this.query(where ? `COUNT ${block} WHERE ${where}` : `COUNT ${block}`, db);
  }

  async health() {
    const res = await fetch(`${this.queryURL}/health`);
    return res.json();
  }

  async status() {
    const res = await fetch(`${this.queryURL}/status`, { headers: this._authHeader() });
    return res.json();
  }

  /**
   * Subscribes to the real-time change stream (Server-Sent Events).
   * `onEvent` is called with the parsed JSON payload of each `change`
   * event: { op, db, block, id, data, timestamp }.
   * Returns an AbortController; call .abort() to stop watching.
   */
  watch(onEvent, { db, block } = {}) {
    const url = new URL(`${this.queryURL}/watch`);
    if (db) url.searchParams.set('db', db);
    if (block) url.searchParams.set('block', block);

    const controller = new AbortController();
    (async () => {
      const res = await fetch(url.toString(), {
        headers: this._authHeader(),
        signal: controller.signal,
      });
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf('\n\n')) !== -1) {
          const chunk = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          const line = chunk.split('\n').find((l) => l.startsWith('data:'));
          if (line) {
            try {
              onEvent(JSON.parse(line.slice(5).trim()));
            } catch (_e) {
              // ignore malformed/heartbeat lines
            }
          }
        }
      }
    })().catch((err) => {
      if (err.name !== 'AbortError') throw err;
    });
    return controller;
  }
}

module.exports = { CaimanDBClient, CaimanDBError };
