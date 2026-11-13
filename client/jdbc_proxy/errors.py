"""Error types mirroring the server's structured error payloads."""


class JdbcProxyError(Exception):
    """Raised for HTTP-level, transport-level and SQL-level failures.

    Attributes:
        message:    human-readable error message (SQL error text for SQL failures).
        status:     HTTP status code of the response, if any.
        sql_state:  JDBC SQLSTATE of the failing statement, if any.
        error_code: vendor-specific error code, if any.
        error_type: server-side category: api_error | sql_error | internal_error.
        caused_by:  chained SQL exception text (batch failures etc.), if any.
    """

    def __init__(self, message, *, status=None, sql_state=None, error_code=None,
                 error_type=None, caused_by=None, raw_error=None):
        super().__init__(message)
        self.message = message
        self.status = status
        self.sql_state = sql_state
        self.error_code = error_code
        self.error_type = error_type
        self.caused_by = caused_by
        self.raw_error = raw_error or {}

    @classmethod
    def from_response(cls, status, payload):
        err = (payload or {}).get("error") or {}
        return cls(
            err.get("message", f"HTTP {status}"),
            status=status,
            sql_state=err.get("sqlState"),
            error_code=err.get("errorCode"),
            error_type=err.get("type"),
            caused_by=err.get("causedBy"),
            raw_error=err,
        )

    def __str__(self):
        parts = [self.message]
        if self.sql_state is not None:
            parts.append(f"[SQLSTATE={self.sql_state}]")
        if self.error_code is not None:
            parts.append(f"[code={self.error_code}]")
        if self.caused_by:
            parts.append(f"caused by: {self.caused_by}")
        if self.error_type == "internal_error" and self.status is None:
            parts.append("(transport error — is the proxy running?)")
        return " ".join(parts)


class ConnectionClosedError(JdbcProxyError):
    """Raised when a connection object is used after close()."""
