"""Thread-safe HTTP transport with per-thread keep-alive connections (http.client)."""

import http.client
import json as _json
import threading
import urllib.parse

from .errors import JdbcProxyError

_USER_AGENT = "jdbc-proxy-client/1.1.0"


class _Transport:
    """JSON-over-HTTP helper shared by sync client and connection objects.

    Each thread keeps its own persistent HTTPConnection (keep-alive), so
    N client threads reuse N sockets instead of opening a TCP connection per
    request. Stale keep-alive connections (server idle timeout) are detected
    and transparently retried for GET requests; POST/DELETE are never retried
    because the statement may already have executed.
    """

    def __init__(self, base_url, api_key=None, timeout=120.0):
        u = urllib.parse.urlparse(base_url if "://" in base_url else "http://" + base_url)
        self.scheme = u.scheme or "http"
        self.host = u.hostname or "127.0.0.1"
        self.port = u.port or (443 if self.scheme == "https" else 80)
        self.prefix = (u.path or "").rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._local = threading.local()

    def _connection(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            if self.scheme == "https":
                conn = http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout)
            else:
                conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            self._local.conn = conn
        return conn

    def _reset(self):
        conn = getattr(self._local, "conn", None)
        self._local.conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def request(self, method, path, body=None):
        data = None
        headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
        if body is not None:
            data = _json.dumps(body, default=str).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        target = self.prefix + path

        status, raw = None, None
        for attempt in (1, 2):
            conn = self._connection()
            try:
                conn.request(method, target, body=data, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                status = resp.status
                break
            except (http.client.HTTPException, OSError) as e:
                self._reset()
                if method != "GET" or attempt == 2:
                    raise JdbcProxyError(
                        f"request to {self.scheme}://{self.host}:{self.port}{target} failed: {e}. "
                        f"Non-GET requests are not auto-retried (the statement may have executed).",
                        error_type="transport_error",
                    ) from None
                # stale keep-alive on a GET: reconnect and retry once

        if status >= 400:
            payload = {}
            try:
                payload = _json.loads(raw.decode("utf-8")) if raw else {}
            except Exception:  # non-JSON body from a proxy in front of the server
                text = raw[:200].decode("utf-8", "replace") if raw else f"HTTP {status}"
                payload = {"error": {"message": text, "type": "http_error"}}
            raise JdbcProxyError.from_response(status, payload) from None
        if not raw:
            return {}
        try:
            return _json.loads(raw.decode("utf-8"))
        except ValueError as e:
            raise JdbcProxyError(f"invalid JSON from proxy: {raw[:120]!r}",
                                 error_type="transport_error") from e
