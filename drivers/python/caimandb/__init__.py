"""
CaimanDB driver for Python.

Talks to CaimanDB's query HTTP server (default port 1555) and,
optionally, its admin HTTP server (default port 1556) for login.
Standard-library only (urllib) -- no external dependencies.

Wire protocol (see docs/api/http-api.md in the CaimanDB repo):
    POST {query_url}/query?query=<NQL>&db=<db>   -> {success, result, db}
    GET  {query_url}/health
    GET  {query_url}/status
    GET  {query_url}/watch?db=&block=            -> Server-Sent Events
    POST {admin_url}/api/v1/auth/login  {username, password}
                                                  -> {token, user, role, expires}

Auth: either HTTP Basic (username/password) or a JWT Bearer token
obtained via login() or supplied directly.
"""

import base64
import json
import urllib.error
import urllib.parse
import urllib.request

__all__ = ["CaimanDBClient", "CaimanDBError"]


class CaimanDBError(Exception):
    def __init__(self, message, status=None, body=None):
        super().__init__(message)
        self.status = status
        self.body = body


class CaimanDBClient:
    def __init__(
        self,
        query_url="http://localhost:1555",
        admin_url="http://localhost:1556",
        username=None,
        password=None,
        token=None,
        db="default",
        timeout=30,
    ):
        self.query_url = query_url.rstrip("/")
        self.admin_url = admin_url.rstrip("/")
        self.username = username
        self.password = password
        self.token = token
        self.default_db = db
        self.timeout = timeout

    # ---- internals --------------------------------------------------

    def _auth_header(self):
        if self.token:
            return {"Authorization": f"Bearer {self.token}"}
        if self.username is not None:
            raw = f"{self.username}:{self.password or ''}".encode("utf-8")
            return {"Authorization": "Basic " + base64.b64encode(raw).decode("ascii")}
        return {}

    def _request(self, method, url, headers=None, body=None):
        headers = {**(headers or {})}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                status = resp.status
        except urllib.error.HTTPError as e:
            raw = e.read()
            status = e.code
        try:
            parsed = json.loads(raw.decode("utf-8")) if raw else {}
        except ValueError:
            parsed = {"raw": raw.decode("utf-8", errors="replace")}
        if status >= 400:
            raise CaimanDBError(
                parsed.get("error") or parsed.get("result") or f"request failed ({status})",
                status,
                parsed,
            )
        return parsed

    # ---- auth ---------------------------------------------------------

    def login(self, username, password):
        """Logs in against the admin server and stores the JWT for subsequent requests."""
        body = self._request(
            "POST", f"{self.admin_url}/api/v1/auth/login", body={"username": username, "password": password}
        )
        self.token = body.get("token")
        self.username = username
        return body  # {token, user, role, expires}

    # ---- raw query ------------------------------------------------------

    def query(self, nql, db=None):
        """Executes a raw NQL command, e.g. 'FIND users WHERE age > 18'."""
        params = urllib.parse.urlencode({"query": nql, "db": db or self.default_db})
        url = f"{self.query_url}/query?{params}"
        body = self._request("POST", url, headers=self._auth_header())
        if body.get("success") is False:
            raise CaimanDBError(body.get("result") or "query failed", body=body)
        return body

    # ---- convenience wrappers over query() -------------------------------

    def insert(self, block, doc_or_docs, db=None):
        """INSERT one document (dict) or several (list of dicts)."""
        payload = json.dumps(doc_or_docs)
        return self.query(f"INSERT {block} {payload}", db)

    def get(self, block, doc_id, db=None):
        """GET a single document by id."""
        return self.query(f"GET {block} {doc_id}", db)

    def find(self, block, where=None, select=None, order=None, limit=None, offset=None, db=None):
        """FIND documents. `where` is a raw NQL WHERE clause string."""
        cmd = f"FIND {block}"
        if select:
            cmd += f" SELECT {', '.join(select)}"
        if where:
            cmd += f" WHERE {where}"
        if order:
            cmd += f" ORDER {order}"
        if limit is not None:
            cmd += f" LIMIT {limit}"
        if offset is not None:
            cmd += f" OFFSET {offset}"
        return self.query(cmd, db)

    def search(self, block, text, exact=False, fuzzy=False, with_score=False, with_matches=False, db=None):
        """SEARCH full-text."""
        cmd = f"SEARCH {block} {json.dumps(text)}"
        if exact:
            cmd += " EXACT"
        if fuzzy:
            cmd += " FUZZY"
        if with_score:
            cmd += " WITH SCORE"
        if with_matches:
            cmd += " WITH MATCHES"
        return self.query(cmd, db)

    def update(self, block, where, set_clause, db=None):
        """UPDATE documents matching a WHERE clause with a raw SET/INC/PUSH clause."""
        return self.query(f"UPDATE {block} WHERE {where} {set_clause}", db)

    def delete(self, block, where, db=None):
        """DELETE documents matching a WHERE clause."""
        return self.query(f"DELETE {block} WHERE {where}", db)

    def count(self, block, where=None, db=None):
        """COUNT documents matching an optional WHERE clause."""
        cmd = f"COUNT {block} WHERE {where}" if where else f"COUNT {block}"
        return self.query(cmd, db)

    def health(self):
        return self._request("GET", f"{self.query_url}/health")

    def status(self):
        return self._request("GET", f"{self.query_url}/status", headers=self._auth_header())

    def watch(self, on_event, db=None, block=None):
        """
        Subscribes to the real-time change stream (Server-Sent Events).
        Blocks the calling thread, calling `on_event(dict)` for each
        `change` event: {op, db, block, id, data, timestamp}. Run this
        in a background thread if you don't want to block.

        Stops when the connection is closed (e.g. server shutdown) or
        the process is interrupted (KeyboardInterrupt).
        """
        params = {}
        if db:
            params["db"] = db
        if block:
            params["block"] = block
        qs = ("?" + urllib.parse.urlencode(params)) if params else ""
        url = f"{self.query_url}/watch{qs}"
        req = urllib.request.Request(url, headers=self._auth_header())
        with urllib.request.urlopen(req, timeout=None) as resp:
            for raw_line in resp:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if line.startswith("data:"):
                    payload = line[len("data:"):].strip()
                    try:
                        on_event(json.loads(payload))
                    except ValueError:
                        pass  # malformed/heartbeat line
