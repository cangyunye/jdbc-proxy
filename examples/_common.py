"""Shared helpers for the jdbc-proxy example scripts.

Every example imports this module first so the `client/` package is on
sys.path regardless of where you run python from.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "client"))

from jdbc_proxy import (AsyncJdbcProxyClient, JdbcProxyClient,  # noqa: E402,F401
                        JdbcProxyError)

PROXY_URL = os.environ.get("JDBC_PROXY_URL", "http://127.0.0.1:8089")

MYSQL = dict(
    url=os.environ.get("MYSQL_URL", "jdbc:mysql://127.0.0.1:3306/default_db"),
    user=os.environ.get("MYSQL_USER", "root"),
    password=os.environ.get("MYSQL_PASSWORD", "root123456"),
)

PG = dict(
    url=os.environ.get("PG_URL", "jdbc:postgresql://127.0.0.1:5432/postgres_db"),
    user=os.environ.get("PG_USER", "postgres"),
    password=os.environ.get("PG_PASSWORD", "postgres123"),
)

ORACLE = dict(
    url=os.environ.get("ORACLE_URL", "jdbc:oracle:thin:@//127.0.0.1:1521/XEPDB1"),
    user=os.environ.get("ORACLE_USER", "scott"),
    password=os.environ.get("ORACLE_PASSWORD", "tiger"),
)


def banner(title: str) -> None:
    print("\n" + "=" * 16, title, "=" * 16)


def open_client() -> JdbcProxyClient:
    """Create a client after checking the proxy is reachable."""
    client = JdbcProxyClient(PROXY_URL)
    try:
        h = client.health()
    except JdbcProxyError:
        print(f"jdbc-proxy 不可达: {PROXY_URL}")
        print("请先启动:  java -jar server/target/jdbc-proxy-server.jar --drivers-dir jars")
        raise SystemExit(1)
    print(f"proxy v{h['version']} — {h['status']} | sessions={h['openConnections']} "
          f"physical={h['physicalConnections']}")
    return client


def show(label, value) -> None:
    print(f"  {label}: {value}")


def cleanup_silently(conn, *statements: str) -> None:
    for sql in statements:
        try:
            conn.execute(sql)
        except JdbcProxyError:
            pass
