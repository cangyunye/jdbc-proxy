#!/usr/bin/env python3
"""01 — 快速上手:连接、查询、写入、探活。

运行:  python3 examples/01_quickstart.py
前提:  代理已启动(见 examples/README.md),MySQL 可用。
"""

from _common import MYSQL, banner, open_client, show


def main() -> None:
    client = open_client()

    banner("1. 打开一个 JDBC 连接(MySQL)")
    conn = client.connect(url=MYSQL["url"], user=MYSQL["user"], password=MYSQL["password"])
    show("连接 id", conn.id)
    show("数据库方言", f"{conn.dialect()['productName']} {conn.dialect()['productVersion']}")

    banner("2. 参数化查询 —— 值自动转成 Python 原生类型")
    r = conn.query("SELECT EMPNO, ENAME, SAL, HIREDATE FROM EMP WHERE DEPTNO = ? ORDER BY EMPNO", [20])
    show("列名", r.column_names)
    for row in r.rows:
        print("   ", row)
    show("首行转字典", r.dict_rows()[0])
    # SAL 是 DECIMAL -> Decimal('1250.00')(字符串传输,精度无损)
    # HIREDATE 是 DATE   -> datetime.date

    banner("3. 写入 + 取自增主键")
    res = conn.execute("INSERT INTO students (name, age) VALUES (?, ?)", ["quickstart", 20])
    show("影响行数", res.update_count)
    new_id = res.generated_keys.first_row()[0]
    show("自增 id", new_id)
    n = conn.query("SELECT COUNT(*) FROM students").scalar()
    show("students 总数", n)

    banner("4. 清理与探活")
    conn.execute("DELETE FROM students WHERE name = ?", ["quickstart"])
    show("ping", conn.ping())

    conn.close()
    client.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
