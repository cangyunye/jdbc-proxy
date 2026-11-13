"""JDBC connection object and result types for jdbc-proxy (sync)."""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, List, Optional, Sequence, Union

from .core import (BatchResult, Column, QueryResult, Typed, _UNSET,
                   build_result, dialect_defaults, normalize_params,
                   rows_to_dicts, shape_columns, shape_foreign_keys,
                   shape_indexes, shape_primary_keys, shape_schemas,
                   shape_tables)
from .errors import ConnectionClosedError, JdbcProxyError

# Per-thread (and per-asyncio-task) transaction state: statements issued on the
# thread that opened a transaction join it; other threads' reads stay independent.
_in_txn: ContextVar = ContextVar("jdbc_proxy_in_txn", default=False)


class JdbcConnection:
    """A logical server-side session backed by a small pool of physical
    connections.

    Concurrency model:
      * Concurrent ``query()`` calls from multiple threads/tasks run in parallel
        on separate physical connections and are scheduled read-first by the
        server. They never see or wait for uncommitted transactions.
      * ``begin()`` / ``transaction()`` pins one physical connection for the
        calling thread; statements issued from *that* thread join the
        transaction and see its uncommitted changes. Reads issued from other
        threads keep working on other connections.
      * ``execute_many`` (batch) and metadata calls are safe to call from any
        thread.
    """

    def __init__(self, transport, connection_id: str, dialect_info: Optional[dict] = None):
        self._t = transport
        self.id = connection_id
        self._dialect_cache = dialect_info
        self._closed = False

    # ------------------------------------------------------------ low level

    def _post(self, path: str, body: Optional[dict] = None) -> dict:
        if self._closed:
            raise ConnectionClosedError(f"connection {self.id} is closed")
        return self._t.request("POST", f"/api/connections/{self.id}{path}", body)

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        if self._closed:
            raise ConnectionClosedError(f"connection {self.id} is closed")
        query = ""
        if params:
            from urllib.parse import urlencode
            query = "?" + urlencode({k: v for k, v in params.items() if v is not None})
        return self._t.request("GET", f"/api/connections/{self.id}{path}{query}")

    # ------------------------------------------------------------ execution

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None, *,
                max_rows: Optional[int] = None,
                fetch_size: Optional[int] = None,
                timeout_seconds: Optional[int] = None,
                intent: Optional[str] = None,
                convert: bool = True) -> QueryResult:
        """Run one statement (query or DML). Returns a QueryResult.

        intent: optional scheduling hint — ``"read"`` runs the statement autocommit
        on a dedicated pool connection (parallel, never blocked by transactions);
        ``"write"`` (default) joins this thread's active transaction if one exists.
        Parameter types supported natively: None, bool, int, float, str,
        Decimal, datetime.date, datetime.time, datetime.datetime, bytes.
        Arbitrary Python objects are sent as JSON text; use Typed(type, value)
        for explicit control (e.g. Typed("json", data) for a PostgreSQL JSONB column).
        """
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
        return build_result(self._post("/execute", body), convert=convert)

    def query(self, sql: str, params: Optional[Sequence[Any]] = None, *,
              intent: Optional[str] = None, **opts) -> QueryResult:
        """Run a SELECT and return rows; raises when the statement is not a query.

        Scheduling: by default query() declares ``intent="read"`` so the server
        runs it autocommit on a dedicated pool connection — concurrent reads are
        parallel and never blocked by transactions opened on other threads.
        Inside a ``transaction()`` block on the current thread, query() joins
        the transaction instead (so you see your own uncommitted changes).
        Pass ``intent="read"`` / ``intent="write"`` explicitly to override.
        """
        effective = intent or ("write" if _in_txn.get() else "read")
        if effective != "write":
            opts["intent"] = effective
        result = self.execute(sql, params, **opts)
        if result.statement_type != "query":
            raise JdbcProxyError(
                f"query() expects a SELECT but got a {result.statement_type} "
                f"(updateCount={result.update_count}); use execute() instead")
        return result

    def execute_many(self, sql: str, batch: Sequence[Sequence[Any]]) -> BatchResult:
        """Run the same parameterized statement repeatedly as a server-side batch."""
        if not batch:
            raise JdbcProxyError("batch must contain at least one parameter row")
        resp = self._post("/batch", {"sql": sql,
                                     "batch": [normalize_params(list(r)) for r in batch]})
        keys = resp.get("generatedKeys")
        return BatchResult(
            update_counts=list(resp.get("updateCounts") or []),
            generated_keys=build_result(keys) if keys else None,
        )

    # ------------------------------------------------------------ transactions

    def begin(self, isolation: Optional[Union[str, int]] = None) -> dict:
        """Start a transaction on this thread (autoCommit=False on a pinned
        physical connection). Statements from this thread now join it."""
        body: Dict[str, Any] = {"action": "begin"}
        if isolation is not None:
            body["isolation"] = isolation
        resp = self._post("/transaction", body)
        _in_txn.set(True)
        return resp

    def commit(self) -> dict:
        resp = self._post("/transaction", {"action": "commit"})
        _in_txn.set(False)
        return resp

    def rollback(self) -> dict:
        resp = self._post("/transaction", {"action": "rollback"})
        _in_txn.set(False)
        return resp

    @contextmanager
    def transaction(self, isolation: Optional[Union[str, int]] = None):
        """Context manager: begin(); commit on success, rollback on any exception.

        isolation: optional isolation level name
            (READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE, READ_UNCOMMITTED, NONE)
        """
        self.begin(isolation)
        try:
            yield self
        except BaseException:
            self.rollback()
            raise
        self.commit()

    def set_auto_commit(self, value: bool) -> dict:
        resp = self._post("/transaction", {"action": "setAutoCommit", "value": value})
        _in_txn.set(not value)
        return resp

    # ------------------------------------------------------------ metadata

    def dialect(self, refresh: bool = False) -> dict:
        """Dialect snapshot for this connection (product name, quoting rules, limits)."""
        if self._dialect_cache is None or refresh:
            self._dialect_cache = self._get("/metadata/dialect")
        return self._dialect_cache

    def _metadata(self, name: str, params: Optional[dict] = None) -> List[Dict[str, Any]]:
        return rows_to_dicts(self._get(f"/metadata/{name}", params))

    def _fill_defaults(self, catalog, schema):
        if catalog is _UNSET or schema is _UNSET:
            dc, ds = dialect_defaults(self.dialect())
            if catalog is _UNSET:
                catalog = dc
            if schema is _UNSET:
                schema = ds
        return catalog, schema

    def list_tables(self, catalog=_UNSET, schema=_UNSET, table=None,
                    types=("TABLE", "VIEW"), max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Tables/views visible to the connection. Defaults are dialect-aware:
        MySQL uses the URL's database as catalog; PostgreSQL/Oracle use the
        default schema. Pass catalog=None / schema=None explicitly to search all."""
        if catalog is _UNSET or schema is _UNSET:
            dc, ds = dialect_defaults(self.dialect())
            if catalog is _UNSET:
                catalog = dc
            if schema is _UNSET:
                schema = ds
        params: Dict[str, Any] = {"catalog": catalog, "schemaPattern": schema, "tablePattern": table}
        if types:
            params["types"] = ",".join(types)
        if max_rows is not None:
            params["max"] = max_rows
        return shape_tables(self._metadata("tables", params))

    def describe_table(self, table: str, schema=_UNSET, catalog=_UNSET,
                       column: Optional[str] = None,
                       max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Column-level structure of a table, ordered by ordinal position."""
        catalog, schema = self._fill_defaults(catalog, schema)
        params: Dict[str, Any] = {
            "catalog": catalog, "schemaPattern": schema,
            "tablePattern": table, "columnPattern": column,
        }
        if max_rows is not None:
            params["max"] = max_rows
        return shape_columns(self._metadata("columns", params))

    def get_primary_keys(self, table: str, schema=_UNSET, catalog=_UNSET) -> List[Dict[str, Any]]:
        catalog, schema = self._fill_defaults(catalog, schema)
        rows = self._metadata("primaryKeys", {"catalog": catalog, "schema": schema, "table": table})
        return shape_primary_keys(rows)

    def get_foreign_keys(self, table: str, schema=_UNSET, catalog=_UNSET,
                         direction: str = "imported") -> List[Dict[str, Any]]:
        """FKs referencing this table (direction="imported", the default) or
        FKs this table references elsewhere (direction="exported")."""
        catalog, schema = self._fill_defaults(catalog, schema)
        rows = self._metadata("foreignKeys", {
            "catalog": catalog, "schema": schema, "table": table, "direction": direction,
        })
        return shape_foreign_keys(rows)

    def get_indexes(self, table: str, schema=_UNSET, catalog=_UNSET,
                    unique_only: bool = False) -> List[Dict[str, Any]]:
        catalog, schema = self._fill_defaults(catalog, schema)
        rows = self._metadata("indexes", {
            "catalog": catalog, "schema": schema, "table": table,
            "unique": str(bool(unique_only)).lower(),
        })
        return shape_indexes(rows)

    def get_schemas(self, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        params = {"max": max_rows} if max_rows is not None else None
        return shape_schemas(self._metadata("schemas", params))

    def get_catalogs(self, max_rows: Optional[int] = None) -> List[str]:
        params = {"max": max_rows} if max_rows is not None else None
        return [r.get("TABLE_CAT") for r in self._metadata("catalogs", params)
                if r.get("TABLE_CAT") is not None]

    def get_table_types(self) -> List[str]:
        return [r.get("TABLE_TYPE") for r in self._metadata("tableTypes")]

    def get_type_info(self, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Vendor type list from DatabaseMetaData.getTypeInfo()."""
        params = {"max": max_rows} if max_rows is not None else None
        return self._metadata("typeInfo", params)

    # ------------------------------------------------------------ lifecycle

    def ping(self) -> bool:
        """Cheap liveness probe against the database."""
        d = self.dialect()
        family = (d.get("dialect") or d.get("productName") or "").lower()
        sql = "SELECT 1 FROM DUAL" if "oracle" in family else "SELECT 1"
        self.execute(sql, max_rows=1)
        return True

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._t.request("DELETE", f"/api/connections/{self.id}")
        except JdbcProxyError:
            pass  # already gone server-side (idle timeout, restart, ...)

    def __enter__(self) -> "JdbcConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        txn = " txn" if _in_txn.get() else ""
        return f"<JdbcConnection {self.id} {state}{txn}>"
