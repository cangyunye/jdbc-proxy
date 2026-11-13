"""Top-level client for the jdbc-proxy HTTP server."""

import threading
from typing import Any, Dict, List, Optional

from .connection import JdbcConnection
from .transport import _Transport


class JdbcProxyClient:
    """Client entry point. Thread-safe: concurrent threads may share one client.

    Args:
        base_url: proxy base URL, e.g. ``http://127.0.0.1:8089``.
        api_key:  value for the ``X-Api-Key`` header, if the server requires one.
        timeout:  per-request timeout in seconds (large reads may need more).
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8089",
                 api_key: Optional[str] = None, timeout: float = 120.0):
        self._t = _Transport(base_url, api_key=api_key, timeout=timeout)
        self._open: List[JdbcConnection] = []
        self._open_lock = threading.Lock()

    # ------------------------------------------------------------ lifecycle

    def health(self) -> Dict[str, Any]:
        """Server liveness + limits: {status, version, openConnections, maxRows, ...}."""
        return self._t.request("GET", "/health")

    def drivers(self) -> List[Dict[str, Any]]:
        """JDBC drivers loaded server-side: [{className, majorVersion, ...}]."""
        return self._t.request("GET", "/drivers").get("drivers") or []

    def connections(self) -> List[Dict[str, Any]]:
        """Open connections on the server: [{id, url, user, createdAt, ...}]."""
        return self._t.request("GET", "/api/connections").get("connections") or []

    def connect(self, url: str, *, user: Optional[str] = None,
                password: Optional[str] = None,
                driver_class: Optional[str] = None,
                properties: Optional[Dict[str, str]] = None,
                auto_commit: bool = True,
                read_only: bool = False,
                isolation: Optional[Any] = None,
                dialect: Optional[str] = None) -> JdbcConnection:
        """Open a JDBC connection on the server.

        Args:
            url:          full JDBC URL, e.g. jdbc:mysql://host:3306/db
            user/password: credentials.
            driver_class: explicit java.sql.Driver class name; auto-detected from
                          the URL when omitted.
            properties:   extra driver connection properties (strings).
            auto_commit:  start in auto-commit mode (default True).
            read_only:    open the connection read-only.
            isolation:    transaction isolation name or JDBC integer level.
            dialect:      explicit dialect declaration for compat-mode databases
                          (OceanBase/OpenGauss), one of:
                          mysql | mariadb | oceanbase_mysql |
                          postgresql | pg | opengauss | gaussdb | oceanbase_pg |
                          oracle | oceanbase_oracle
                          Omit for auto-detection from the product name/URL.

        Returns:
            JdbcConnection — also usable as a context manager.
        """
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
        resp = self._t.request("POST", "/api/connections", body)
        # resp["dialect"] is the resolved family name (str); the full dialect
        # snapshot dict comes from metadata later
        cached = resp.get("dialect") if isinstance(resp.get("dialect"), dict) else None
        conn = JdbcConnection(self._t, resp["connectionId"], cached)
        with self._open_lock:
            self._open.append(conn)
        return conn

    def close(self) -> None:
        """Close every connection opened through this client (errors ignored)."""
        with self._open_lock:
            conns = list(self._open)
            self._open.clear()
        for conn in conns:
            conn.close()

    def __enter__(self) -> "JdbcProxyClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<JdbcProxyClient {self._t.base_url} open={len(self._open)}>"
