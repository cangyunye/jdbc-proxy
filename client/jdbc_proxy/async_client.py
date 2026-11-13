"""Asyncio client for jdbc-proxy — the same HTTP protocol, await-based API.

Pure standard library (asyncio streams + a small keep-alive connection pool),
so ``asyncio.gather`` over many queries runs truly concurrently against the
server's per-session pool.
"""

import asyncio
import json as _json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urlencode

from .core import (BatchResult, QueryResult, Typed, _UNSET, build_result,
                   dialect_defaults, normalize_params, rows_to_dicts,
                   shape_columns, shape_foreign_keys, shape_indexes,
                   shape_primary_keys, shape_schemas, shape_tables)
from .errors import ConnectionClosedError, JdbcProxyError

_in_txn: ContextVar = ContextVar("jdbc_proxy_async_in_txn", default=False)

_USER_AGENT = "jdbc-proxy-client-async/1.1.0"


class _AsyncHttp:
    """Minimal HTTP/1.1 client over asyncio streams with a keep-alive pool."""

    def __init__(self, base_url: str, api_key: Optional[str], timeout: float,
                 max_connections: int):
        u = _parse_url(base_url)
        self.scheme, self.host, self.port, self.prefix = u
        self.api_key = api_key
        self.timeout = timeout
        self._pool: List[Any] = []
        self._sem = asyncio.Semaphore(max_connections)

    async def _acquire(self):
        await self._sem.acquire()
        if self._pool:
            return self._pool.pop()
        try:
            ssl_ctx = None if self.scheme == "http" else True  # system default CA
            reader, writer = await asyncio.open_connection(
                self.host, self.port, ssl=ssl_ctx, limit=2 ** 22)
            return reader, writer
        except BaseException:
            self._sem.release()
            raise

    def _settle(self, reader, writer, reusable: bool) -> None:
        if reusable and not writer.is_closing():
            self._pool.append((reader, writer))
        else:
            try:
                writer.close()
            except Exception:
                pass
        self._sem.release()

    async def close(self) -> None:
        """Close every pooled keep-alive connection."""
        while self._pool:
            _, w = self._pool.pop()
            try:
                w.close()
            except Exception:
                pass

    async def request(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        data = None
        headers = {"User-Agent": _USER_AGENT, "Accept": "application/json",
                   "Host": f"{self.host}:{self.port}", "Connection": "keep-alive"}
        if body is not None:
            data = _json.dumps(body, default=str).encode("utf-8")
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(data))  # required for HTTP/1.1 POST bodies
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        target = self.prefix + path

        head = f"{method} {target} HTTP/1.1\r\n" + \
            "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
        payload = head.encode("latin-1") + (data or b"")

        last_error: Optional[Exception] = None
        for attempt in (1, 2):
            reader, writer = await self._acquire()
            settled = False
            try:
                writer.write(payload)
                await asyncio.wait_for(writer.drain(), self.timeout)
                status, resp_headers = await self._read_head(reader)
                raw, reusable = await self._read_body(reader, resp_headers)
                settled = True
                self._settle(reader, writer, reusable)
                if status >= 400:
                    raise JdbcProxyError.from_response(status, _parse_json_or(raw, status))
                return _parse_json_or(raw, status) if raw else {}
            except JdbcProxyError:
                if not settled:
                    self._settle(reader, writer, False)
                raise
            except (OSError, asyncio.IncompleteReadError, ValueError) as e:
                if not settled:
                    self._settle(reader, writer, False)
                last_error = e
                if method != "GET" or attempt == 2:
                    raise JdbcProxyError(
                        f"request to {self.scheme}://{self.host}:{self.port}{target} failed: {e}. "
                        f"Non-GET requests are not auto-retried (the statement may have executed).",
                        error_type="transport_error",
                    ) from None
                # stale keep-alive on a GET: reconnect and retry once
        raise JdbcProxyError(str(last_error), error_type="transport_error")

    async def _read_head(self, reader) -> Any:
        status_line = await asyncio.wait_for(reader.readline(), self.timeout)
        if not status_line:
            raise ConnectionResetError("server closed the connection before a response")
        parts = status_line.decode("latin-1").split(" ", 2)
        if len(parts) < 2:
            raise ValueError(f"bad HTTP status line: {status_line!r}")
        status = int(parts[1])
        headers: Dict[str, str] = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), self.timeout)
            if line in (b"\r\n", b"\n", b""):
                break
            k, _, v = line.decode("latin-1").partition(":")
            headers[k.strip().lower()] = v.strip()
        return status, headers

    async def _read_body(self, reader, headers: Dict[str, str]) -> Any:
        close_after = headers.get("connection", "").lower() == "close"
        if "content-length" in headers:
            n = int(headers["content-length"])
            raw = await asyncio.wait_for(reader.readexactly(n), self.timeout) if n else b""
            return raw, not close_after
        if headers.get("transfer-encoding", "").lower().startswith("chunked"):
            buf = bytearray()
            while True:
                size_line = await asyncio.wait_for(reader.readline(), self.timeout)
                size = int(size_line.split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    while True:  # trailer section, ends with a blank line
                        trailer = await asyncio.wait_for(reader.readline(), self.timeout)
                        if trailer in (b"\r\n", b"\n", b""):
                            break
                    return bytes(buf), not close_after
                buf += await asyncio.wait_for(reader.readexactly(size), self.timeout)
                await asyncio.wait_for(reader.readexactly(2), self.timeout)
        raw = await asyncio.wait_for(reader.read(), self.timeout)
        return raw, False


def _parse_url(base_url: str):
    from urllib.parse import urlparse
    u = urlparse(base_url if "://" in base_url else "http://" + base_url)
    scheme = u.scheme or "http"
    host = u.hostname or "127.0.0.1"
    port = u.port or (443 if scheme == "https" else 80)
    return scheme, host, port, (u.path or "").rstrip("/")


def _parse_json_or(raw: bytes, status: int) -> dict:
    try:
        return _json.loads(raw.decode("utf-8")) if raw else {}
    except Exception:
        text = raw[:200].decode("utf-8", "replace") if raw else f"HTTP {status}"
        return {"error": {"message": text, "type": "http_error"}}


class AsyncJdbcConnection:
    """Async handle to one logical server-side session (pooled physical
    connections server-side). Concurrent ``query()`` tasks run in parallel and
    read-first; ``transaction()`` joins the calling task to its transaction."""

    def __init__(self, transport: _AsyncHttp, connection_id: str, dialect_info: Optional[dict] = None):
        self._t = transport
        self.id = connection_id
        self._dialect_cache = dialect_info
        self._closed = False

    async def _post(self, path: str, body: Optional[dict] = None) -> dict:
        if self._closed:
            raise ConnectionClosedError(f"connection {self.id} is closed")
        return await self._t.request("POST", f"/api/connections/{self.id}{path}", body)

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        if self._closed:
            raise ConnectionClosedError(f"connection {self.id} is closed")
        query = ""
        if params:
            query = "?" + urlencode({k: v for k, v in params.items() if v is not None})
        return await self._t.request("GET", f"/api/connections/{self.id}{path}{query}")

    # ------------------------------------------------------------ execution

    async def execute(self, sql: str, params: Optional[Sequence[Any]] = None, *,
                      max_rows: Optional[int] = None,
                      fetch_size: Optional[int] = None,
                      timeout_seconds: Optional[int] = None,
                      intent: Optional[str] = None,
                      convert: bool = True) -> QueryResult:
        body: Dict[str, Any] = {"sql": sql}
        if params:
            body["params"] = normalize_params(params)
        if max_rows is not None:
            body["maxRows"] = max_rows
        if fetch_size is not None:
            body["fetchSize"] = fetch_size
        if timeout_seconds is not None:
            body["timeoutSeconds"] = timeout_seconds
        if intent:
            body["intent"] = intent
        return build_result(await self._post("/execute", body), convert=convert)

    async def query(self, sql: str, params: Optional[Sequence[Any]] = None, *,
                    intent: Optional[str] = None, **opts) -> QueryResult:
        """Run a SELECT; defaults to ``intent="read"`` (parallel, read-first)
        unless inside a ``transaction()`` on the current task."""
        effective = intent or ("write" if _in_txn.get() else "read")
        if effective != "write":
            opts["intent"] = effective
        result = await self.execute(sql, params, **opts)
        if result.statement_type != "query":
            raise JdbcProxyError(
                f"query() expects a SELECT but got a {result.statement_type} "
                f"(updateCount={result.update_count}); use execute() instead")
        return result

    async def execute_many(self, sql: str, batch: Sequence[Sequence[Any]]) -> BatchResult:
        if not batch:
            raise JdbcProxyError("batch must contain at least one parameter row")
        resp = await self._post("/batch", {"sql": sql,
                                           "batch": [normalize_params(list(r)) for r in batch]})
        keys = resp.get("generatedKeys")
        return BatchResult(
            update_counts=list(resp.get("updateCounts") or []),
            generated_keys=build_result(keys) if keys else None,
        )

    # ------------------------------------------------------------ transactions

    async def begin(self, isolation: Optional[Union[str, int]] = None) -> dict:
        body: Dict[str, Any] = {"action": "begin"}
        if isolation is not None:
            body["isolation"] = isolation
        resp = await self._post("/transaction", body)
        _in_txn.set(True)
        return resp

    async def commit(self) -> dict:
        resp = await self._post("/transaction", {"action": "commit"})
        _in_txn.set(False)
        return resp

    async def rollback(self) -> dict:
        resp = await self._post("/transaction", {"action": "rollback"})
        _in_txn.set(False)
        return resp

    @asynccontextmanager
    async def transaction(self, isolation: Optional[Union[str, int]] = None):
        await self.begin(isolation)
        try:
            yield self
        except BaseException:
            await self.rollback()
            raise
        await self.commit()

    async def set_auto_commit(self, value: bool) -> dict:
        resp = await self._post("/transaction", {"action": "setAutoCommit", "value": value})
        _in_txn.set(not value)
        return resp

    # ------------------------------------------------------------ metadata

    async def dialect(self, refresh: bool = False) -> dict:
        if self._dialect_cache is None or refresh:
            self._dialect_cache = await self._get("/metadata/dialect")
        return self._dialect_cache

    async def _metadata(self, name: str, params: Optional[dict] = None) -> List[Dict[str, Any]]:
        return rows_to_dicts(await self._get(f"/metadata/{name}", params))

    async def _fill_defaults(self, catalog, schema):
        if catalog is _UNSET or schema is _UNSET:
            dc, ds = dialect_defaults(await self.dialect())
            if catalog is _UNSET:
                catalog = dc
            if schema is _UNSET:
                schema = ds
        return catalog, schema

    async def list_tables(self, catalog=_UNSET, schema=_UNSET, table=None,
                          types=("TABLE", "VIEW"), max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        if catalog is _UNSET or schema is _UNSET:
            dc, ds = dialect_defaults(await self.dialect())
            if catalog is _UNSET:
                catalog = dc
            if schema is _UNSET:
                schema = ds
        params: Dict[str, Any] = {"catalog": catalog, "schemaPattern": schema, "tablePattern": table}
        if types:
            params["types"] = ",".join(types)
        if max_rows is not None:
            params["max"] = max_rows
        return shape_tables(await self._metadata("tables", params))

    async def describe_table(self, table: str, schema=_UNSET, catalog=_UNSET,
                             column: Optional[str] = None,
                             max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        catalog, schema = await self._fill_defaults(catalog, schema)
        params: Dict[str, Any] = {
            "catalog": catalog, "schemaPattern": schema,
            "tablePattern": table, "columnPattern": column,
        }
        if max_rows is not None:
            params["max"] = max_rows
        return shape_columns(await self._metadata("columns", params))

    async def get_primary_keys(self, table: str, schema=_UNSET, catalog=_UNSET) -> List[Dict[str, Any]]:
        catalog, schema = await self._fill_defaults(catalog, schema)
        rows = await self._metadata("primaryKeys", {"catalog": catalog, "schema": schema, "table": table})
        return shape_primary_keys(rows)

    async def get_foreign_keys(self, table: str, schema=_UNSET, catalog=_UNSET,
                               direction: str = "imported") -> List[Dict[str, Any]]:
        catalog, schema = await self._fill_defaults(catalog, schema)
        rows = await self._metadata("foreignKeys", {
            "catalog": catalog, "schema": schema, "table": table, "direction": direction,
        })
        return shape_foreign_keys(rows)

    async def get_indexes(self, table: str, schema=_UNSET, catalog=_UNSET,
                          unique_only: bool = False) -> List[Dict[str, Any]]:
        catalog, schema = await self._fill_defaults(catalog, schema)
        rows = await self._metadata("indexes", {
            "catalog": catalog, "schema": schema, "table": table,
            "unique": str(bool(unique_only)).lower(),
        })
        return shape_indexes(rows)

    async def get_schemas(self, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        params = {"max": max_rows} if max_rows is not None else None
        return shape_schemas(await self._metadata("schemas", params))

    async def get_catalogs(self, max_rows: Optional[int] = None) -> List[str]:
        params = {"max": max_rows} if max_rows is not None else None
        rows = await self._metadata("catalogs", params)
        return [r.get("TABLE_CAT") for r in rows if r.get("TABLE_CAT") is not None]

    async def get_table_types(self) -> List[str]:
        rows = await self._metadata("tableTypes")
        return [r.get("TABLE_TYPE") for r in rows]

    async def get_type_info(self, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        params = {"max": max_rows} if max_rows is not None else None
        return await self._metadata("typeInfo", params)

    # ------------------------------------------------------------ lifecycle

    async def ping(self) -> bool:
        d = await self.dialect()
        family = (d.get("dialect") or d.get("productName") or "").lower()
        sql = "SELECT 1 FROM DUAL" if "oracle" in family else "SELECT 1"
        await self.execute(sql, max_rows=1)
        return True

    def is_closed(self) -> bool:
        return self._closed

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._t.request("DELETE", f"/api/connections/{self.id}")
        except JdbcProxyError:
            pass

    async def __aenter__(self) -> "AsyncJdbcConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<AsyncJdbcConnection {self.id} {state}>"


class AsyncJdbcProxyClient:
    """Async client entry point.

    Example::

        async with AsyncJdbcProxyClient("http://127.0.0.1:8089") as client:
            conn = await client.connect(url="jdbc:postgresql://...", user="u", password="p")
            results = await asyncio.gather(*[conn.query("SELECT ...") for _ in range(8)])
            await conn.close()
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8089",
                 api_key: Optional[str] = None, timeout: float = 120.0,
                 max_http_connections: int = 16):
        self._t = _AsyncHttp(base_url, api_key=api_key, timeout=timeout,
                             max_connections=max_http_connections)
        self._open: List[AsyncJdbcConnection] = []

    async def health(self) -> Dict[str, Any]:
        return await self._t.request("GET", "/health")

    async def drivers(self) -> List[Dict[str, Any]]:
        return (await self._t.request("GET", "/drivers")).get("drivers") or []

    async def connections(self) -> List[Dict[str, Any]]:
        return (await self._t.request("GET", "/api/connections")).get("connections") or []

    async def connect(self, url: str, *, user: Optional[str] = None,
                      password: Optional[str] = None,
                      driver_class: Optional[str] = None,
                      properties: Optional[Dict[str, str]] = None,
                      auto_commit: bool = True,
                      read_only: bool = False,
                      isolation: Optional[Any] = None,
                      dialect: Optional[str] = None) -> AsyncJdbcConnection:
        body: Dict[str, Any] = {"url": url, "autoCommit": auto_commit, "readOnly": read_only}
        if user is not None:
            body["user"] = user
        if password is not None:
            body["password"] = password
        if driver_class is not None:
            body["driverClass"] = driver_class
        if properties:
            body["properties"] = {str(k): str(v) for k, v in properties.items()}
        if isolation is not None:
            body["isolation"] = isolation
        if dialect is not None:
            body["dialect"] = dialect
        resp = await self._t.request("POST", "/api/connections", body)
        cached = resp.get("dialect") if isinstance(resp.get("dialect"), dict) else None
        conn = AsyncJdbcConnection(self._t, resp["connectionId"], cached)
        self._open.append(conn)
        return conn

    async def close(self) -> None:
        for conn in list(self._open):
            await conn.close()
        self._open.clear()
        await self._t.close()

    async def __aenter__(self) -> "AsyncJdbcProxyClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"<AsyncJdbcProxyClient {self._t.host}:{self._t.port} open={len(self._open)}>"
