#!/usr/bin/env python3
"""02 — 参数与类型:Python 原生类型直传,Typed 处理严格方言。

运行:  python3 examples/02_types_and_params.py
前提:  MySQL + PostgreSQL 可用。
"""

from datetime import date, datetime, time
from decimal import Decimal

from _common import MYSQL, PG, banner, open_client, show
from jdbc_proxy import Typed


def mysql_demo(client) -> None:
    banner("MySQL:原生类型直传(自动映射为安全的传输格式)")
    with client.connect(**MYSQL) as conn:
        conn.execute("DROP TABLE IF EXISTS jp_types_demo")
        conn.execute(
            "CREATE TABLE jp_types_demo ("
            "id INT PRIMARY KEY, price DECIMAL(24,4), day DATE, ts TIMESTAMP(6), "
            "seen TIME(6), blob_col BLOB, flag BOOLEAN, name VARCHAR(100))")
        conn.execute("DELETE FROM jp_types_demo")

        # Decimal / date / datetime / time / bytes / bool 都是普通 Python 对象
        payload = [
            1,
            Decimal("123456789012345678.9012"),        # 高精度金额,精度无损
            date(2026, 11, 12),
            datetime(2026, 3, 14, 15, 9, 26, 535897),  # 微秒
            time(13, 30, 5, 123456),
            b"\x00\x01\xffbinary",                      # bytes -> BLOB
            True,
            "中文与 emoji 🦄",
        ]
        conn.execute(
            "INSERT INTO jp_types_demo VALUES (?, ?, ?, ?, ?, ?, ?, ?)", payload)

        r = conn.query("SELECT * FROM jp_types_demo WHERE id = 1")
        for col, value in zip(r.columns, r.rows[0]):
            show(col.label, repr(value))
        conn.execute("DROP TABLE jp_types_demo")


def pg_demo(client) -> None:
    banner("PostgreSQL:严格类型用 Typed 提示(JSONB / 显式声明)")
    with client.connect(**PG) as conn:
        conn.execute(
            'DROP TABLE IF EXISTS jp_types_pg')
        conn.execute(
            'CREATE TABLE jp_types_pg (id INT PRIMARY KEY, '
            '"name" TEXT, meta JSONB, tzt TIMESTAMPTZ, price NUMERIC(12,2))')
        conn.execute("DELETE FROM jp_types_pg")

        # 1) JSONB:Typed("json", ...) —— dict 自动序列化
        conn.execute(
            'INSERT INTO jp_types_pg (id, "name", meta) VALUES (?, ?, ?)',
            [1, "widget", Typed("json", {"color": "red", "tags": [1, 2]})])

        # 2) 带时区的时间戳:tz-aware datetime 自动走 timestamptz 通道
        from datetime import timedelta, timezone
        dt = datetime(2026, 6, 1, 12, 0, tzinfo=timezone(timedelta(hours=8)))
        conn.execute(
            'INSERT INTO jp_types_pg (id, tzt, price) VALUES (?, ?, ?)',
            [2, dt, Decimal("19.99")])

        row1 = conn.query('SELECT meta FROM jp_types_pg WHERE id = 1').scalar()
        row2 = conn.query('SELECT tzt, price FROM jp_types_pg WHERE id = 2').first_row()
        show("JSONB 读回", row1)
        show("timestamptz 读回", row2[0])   # datetime(带 tzinfo),同一时刻
        show("NUMERIC 读回", row2[1])       # Decimal('19.99')

        # 3) 显式类型提示也可以直接写 dict 形式(其他语言的等价写法)
        conn.execute('INSERT INTO jp_types_pg (id, price) VALUES (?, ?)',
                     [3, {"type": "decimal", "value": "77.10"}])
        show("decimal 提示读回",
             conn.query('SELECT price FROM jp_types_pg WHERE id = 3').scalar())
        conn.execute("DROP TABLE jp_types_pg")


def main() -> None:
    client = open_client()
    mysql_demo(client)
    pg_demo(client)
    client.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
