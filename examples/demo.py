#!/usr/bin/env python3
"""End-to-end demo of the jdbc-proxy Python client.

Prerequisites:
  1. Start the proxy (from the repo root):
       java -jar server/target/jdbc-proxy-server.jar --drivers-dir jars --port 8089
  2. MySQL + PostgreSQL running (docker compose up -d)

Run:
  python3 examples/demo.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "client"))

from decimal import Decimal  # noqa: E402
from datetime import date    # noqa: E402

from jdbc_proxy import JdbcProxyClient, Typed  # noqa: E402

PROXY = os.environ.get("JDBC_PROXY_URL", "http://127.0.0.1:8089")


def mysql_demo(client: JdbcProxyClient) -> None:
    print("=" * 30, "MySQL", "=" * 30)
    with client.connect(
        url=os.environ.get("MYSQL_URL", "jdbc:mysql://127.0.0.1:3306/default_db"),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "root123456"),
    ) as conn:
        # 1. query with parameters; values come back as native Python types
        r = conn.query("SELECT EMPNO, ENAME, SAL, HIREDATE FROM EMP WHERE DEPTNO = ? ORDER BY EMPNO", [20])
        print(f"query: {r.column_names}")
        for row in r.rows:
            print("   ", row)

        # 2. metadata: dialect-aware table listing / column structure
        print("tables like 'st%':", [t["name"] for t in conn.list_tables(table="st%")])
        print("EMP columns:", [(c["name"], c["type"]) for c in conn.describe_table("EMP")])
        print("EMP primary key:", conn.get_primary_keys("EMP"))
        d = conn.dialect()
        print(f"dialect: {d['productName']} {d['productVersion']}, quote={d['identifierQuoteString']!r}")

        # 3. insert + generated keys + batch
        ins = conn.execute("INSERT INTO students (name, age) VALUES (?, ?)", ["demo", 42])
        print("inserted 1 row, generated id:", ins.generated_keys.first_row()[0])
        batch = conn.execute_many("INSERT INTO students (name, age) VALUES (?, ?)",
                                  [["demo-a", 1], ["demo-b", 2], ["demo-c", 3]])
        print("batch update counts:", batch.update_counts)

        # 4. transaction: committed on success, rolled back on exception
        with conn.transaction():
            conn.execute("INSERT INTO students (name) VALUES (?)", ["demo-txn"])
        print("txn commit ok")
        try:
            with conn.transaction():
                conn.execute("INSERT INTO students (name) VALUES (?)", ["demo-txn2"])
                raise RuntimeError("simulated failure")
        except RuntimeError:
            print("txn rollback ok")
        n = conn.query("SELECT COUNT(*) FROM students WHERE name LIKE ?", ["demo%"]).scalar()
        print("rows with prefix demo%:", n)
        conn.execute("DELETE FROM students WHERE name LIKE ?", ["demo%"])


def postgres_demo(client: JdbcProxyClient) -> None:
    print("=" * 30, "PostgreSQL", "=" * 30)
    with client.connect(
        url=os.environ.get("PG_URL", "jdbc:postgresql://127.0.0.1:5432/postgres_db"),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", "postgres123"),
    ) as conn:
        # strict-typing friendly: Decimal / date auto-wrapped as typed params
        conn.execute('CREATE TABLE IF NOT EXISTS jp_demo ('
                     'id SERIAL PRIMARY KEY, "name" TEXT, price NUMERIC(8,2), day DATE, meta JSONB)')
        conn.execute('INSERT INTO jp_demo ("name", price, day, meta) VALUES (?, ?, ?, ?)',
                     ["widget", Decimal("19.99"), date(2026, 11, 12), Typed("json", {"color": "red", "tags": [1, 2]})])
        r = conn.query('SELECT id, "name", price, day, meta FROM jp_demo ORDER BY id DESC LIMIT 1')
        print("query:", r.dict_rows()[-1])

        # INSERT ... RETURNING — returned rows land in generated_keys
        ins = conn.execute('INSERT INTO jp_demo ("name") VALUES (?) RETURNING id', ["gizmo"])
        print("RETURNING id:", ins.generated_keys.first_row()[0])

        print("tables in public:", [t["name"] for t in conn.list_tables(table="jp_demo")])
        print("columns:", [(c["name"], c["type"]) for c in conn.describe_table("jp_demo")])

        conn.execute("DELETE FROM jp_demo")
        print("cleaned up")


def main() -> None:
    with JdbcProxyClient(PROXY) as client:
        health = client.health()
        print(f"proxy {health['service']} v{health['version']} — {health['status']}")
        print("loaded drivers:")
        for drv in client.drivers():
            print(f"   {drv['className']} {drv['majorVersion']}.{drv['minorVersion']}")
        mysql_demo(client)
        postgres_demo(client)
        print("all good.")


if __name__ == "__main__":
    main()
