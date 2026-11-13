#!/usr/bin/env python3
"""07 — 异步:asyncio 客户端,gather 并行、异步事务。

运行:  python3 examples/07_async.py
前提:  MySQL + PostgreSQL 可用。

AsyncJdbcProxyClient 是纯 asyncio 实现(标准库,无第三方依赖),
内置 HTTP keep-alive 连接池;await 一条连接后可以随便 gather。
"""

import asyncio
from datetime import datetime

from _common import MYSQL, PG, banner, open_client, show, PROXY_URL

from jdbc_proxy import AsyncJdbcProxyClient


async def mysql_part() -> None:
    banner("MySQL:asyncio.gather 并行查询")
    async with AsyncJdbcProxyClient(PROXY_URL) as client:
        conn = await client.connect(**MYSQL)
        try:
            t0 = asyncio.get_event_loop().time()
            results = await asyncio.gather(*[
                conn.query("SELECT SLEEP(0.3), CONNECTION_ID()") for _ in range(8)])
            elapsed = asyncio.get_event_loop().time() - t0
            pids = {r.first_row()[1] for r in results}
            print(f"   8 个并发查询耗时 {elapsed:.2f}s(串行约 2.4s+),"
                  f"落在 {len(pids)} 条不同物理连接上")

            banner("MySQL:异步写入 + 自增键")
            r = await conn.execute("INSERT INTO students (name, age) VALUES (?, ?)",
                                   ["async-demo", 30])
            show("自增 id", r.generated_keys.first_row()[0])
            await conn.execute("DELETE FROM students WHERE name = ?", ["async-demo"])
        finally:
            await conn.close()


async def pg_part() -> None:
    banner("PostgreSQL:异步事务(gather 中读自己的未提交数据)")
    async with AsyncJdbcProxyClient(PROXY_URL) as client:
        conn = await client.connect(**PG)
        try:
            await conn.execute(
                'DROP TABLE IF EXISTS jp_async_demo')
            await conn.execute(
                'CREATE TABLE jp_async_demo '
                '(id SERIAL PRIMARY KEY, "name" TEXT, created_at TIMESTAMPTZ)')
            await conn.execute("DELETE FROM jp_async_demo")

            async with conn.transaction(isolation="READ_COMMITTED"):
                await conn.execute(
                    'INSERT INTO jp_async_demo ("name", created_at) VALUES (?, ?)',
                    ["async-txn", datetime.now()])
                # 同一 task 内的 query() 加入事务,能看到自己的未提交行
                inside = (await conn.query('SELECT COUNT(*) FROM jp_async_demo')).scalar()
                show("事务内 count", inside)  # 1

            show("提交后 count",
                 (await conn.query('SELECT COUNT(*) FROM jp_async_demo')).scalar())

            banner("PostgreSQL:异常回滚 + 并行 gather")
            try:
                async with conn.transaction():
                    await conn.execute('INSERT INTO jp_async_demo ("name") VALUES (?)', ["ghost"])
                    raise RuntimeError("模拟失败")
            except RuntimeError:
                show("回滚后 count",
                     (await conn.query('SELECT COUNT(*) FROM jp_async_demo')).scalar())  # 1

            rows = await asyncio.gather(*[
                conn.query("SELECT pg_sleep(0.2), pg_backend_pid()") for _ in range(8)])
            show("并行后端进程数", len({r.first_row()[1] for r in rows}))
            await conn.execute("DROP TABLE jp_async_demo")
        finally:
            await conn.close()


def main() -> None:
    # 先确认代理可达(与同步示例一致的使用体验)
    client = open_client()
    client.close()

    asyncio.run(mysql_part())
    asyncio.run(pg_part())
    print("\ndone.")


if __name__ == "__main__":
    main()
