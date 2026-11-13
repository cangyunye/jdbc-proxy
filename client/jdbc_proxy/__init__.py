"""Python client for jdbc-proxy.

Pure standard library — no third-party dependencies.

Example::

    from jdbc_proxy import JdbcProxyClient

    with JdbcProxyClient("http://127.0.0.1:8089") as client:
        with client.connect(
            url="jdbc:mysql://127.0.0.1:3306/default_db",
            user="root", password="secret",
        ) as conn:
            result = conn.query("SELECT * FROM EMP WHERE deptno > ?", [10])
            for row in result.dict_rows():
                print(row)
"""

from .async_client import AsyncJdbcConnection, AsyncJdbcProxyClient
from .client import JdbcProxyClient
from .connection import BatchResult, Column, JdbcConnection, QueryResult, Typed
from .errors import JdbcProxyError

__all__ = [
    "JdbcProxyClient",
    "JdbcConnection",
    "QueryResult",
    "BatchResult",
    "Column",
    "Typed",
    "JdbcProxyError",
    "AsyncJdbcProxyClient",
    "AsyncJdbcConnection",
]

__version__ = "1.1.0"
