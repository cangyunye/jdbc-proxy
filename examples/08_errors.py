#!/usr/bin/env python3
"""08 — 错误处理与生命周期:结构化 SQL 错误、连接生命周期、常见陷阱。

运行:  python3 examples/08_errors.py
前提:  MySQL 可用。
"""

import time

from _common import MYSQL, banner, open_client, show

from jdbc_proxy import JdbcProxyClient, JdbcProxyError
from jdbc_proxy.errors import ConnectionClosedError


def main() -> None:
    client = open_client()

    banner("1. SQL 错误是结构化的:sqlState + vendor code")
    with client.connect(**MYSQL) as conn:
        try:
            conn.execute("SELECT * FROM definitely_not_a_table_xyz")
        except JdbcProxyError as e:
            show("message", e.message)
            show("sqlState", e.sql_state)      # 42S02: 表不存在
            show("errorCode", e.error_code)    # 1146: MySQL vendor code
            show("str(e)", str(e))

        banner("2. query() 拒绝 DML —— 防呆")
        try:
            conn.query("INSERT INTO students (name) VALUES (?)", ["oops"])
        except JdbcProxyError as e:
            show("错误", str(e))

        banner("3. 批量失败会带回已完成行的计数")
        try:
            conn.execute_many(
                "INSERT INTO students (id, name) VALUES (?, ?)",
                [[900001, "ok"], [900001, "duplicate-pk"]])
        except JdbcProxyError as e:
            show("sqlState", e.sql_state)      # 23000: 完整性冲突
            show("causedBy", (e.caused_by or "")[:80])

        banner("4. 连接生命周期")
        c2 = client.connect(**MYSQL)
        show("连接打开", c2.id)
        show("服务端列表", [c["id"] for c in client.connections()][:3])
        c2.close()
        show("关闭后 is_closed", c2.is_closed())
        try:
            c2.ping()
        except ConnectionClosedError as e:
            show("已关连接再使用", str(e))

        banner("5. 服务端空闲回收 / 客户端消失 → 未知 id")
        ghost = JdbcProxyClient(__import__("_common").PROXY_URL)
        gconn = ghost.connect(**MYSQL)
        ghost._t.request("DELETE", f"/api/connections/{gconn.id}")  # 模拟外部关闭
        try:
            gconn.ping()
        except JdbcProxyError as e:
            show("服务端 404", str(e))

        banner("6. ping 与自动重连探测")
        show("ping", conn.ping())  # 主连接仍然健康
        time.sleep(0.1)

    banner("7. 代理不可达时的表现")
    bad = JdbcProxyClient("http://127.0.0.1:59999", timeout=2)
    try:
        bad.health()
    except JdbcProxyError as e:
        show("transport error", str(e))

    client.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
