#!/usr/bin/env python3
"""09 — Oracle:方言自动识别、显式声明、Oracle 特有的取键模式。

运行:  python3 examples/09_oracle.py
前提:  docker compose up -d oracle(首次初始化需几分钟;SCOTT 由 oracle/ 初始化脚本创建)

要点:
  * 方言自动识别:productName=Oracle → family=oracle;
  * 多兼容模式数据库(OceanBase 的 MySQL/Oracle 模式、OpenGauss)URL 无法区分,
    connect(dialect=...) 显式声明,服务端所有方言相关行为按声明执行;
  * Oracle JDBC 的 getGeneratedKeys 返回 ROWID 伪列(而非自增值),
    标准两步法:先拿 rowid,再回查真实主键。
"""

from decimal import Decimal

from _common import ORACLE, banner, cleanup_silently, open_client, show

from jdbc_proxy import JdbcProxyError


def main() -> None:
    client = open_client()

    banner("1. 连接 Oracle(方言自动识别)")
    conn = client.connect(url=ORACLE["url"], user=ORACLE["user"], password=ORACLE["password"])
    d = conn.dialect()
    show("productName", d["productName"])
    show("解析出的方言(family)", d["dialect"])            # oracle
    show("声明方式", d["dialectHint"])                     # auto
    show("ping(FROM DUAL)", conn.ping())

    banner("2. SCOTT 示例数据")
    rows = conn.query("SELECT EMPNO, ENAME, SAL, HIREDATE FROM EMP WHERE DEPTNO = ? ORDER BY EMPNO",
                      [20]).dict_rows()
    for r in rows:
        print("   ", r)

    banner("3. 建表 + 自增主键(IDENTITY)+ 全类型往返")
    cleanup_silently(conn, "DROP TABLE JP_EXAMPLE PURGE")
    conn.execute(
        "CREATE TABLE JP_EXAMPLE ("
        "id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
        "name VARCHAR2(50), price NUMBER(10,2), "
        "created TIMESTAMP(6), bio CLOB, avatar BLOB)")

    ins = conn.execute("INSERT INTO JP_EXAMPLE (name, price, created, bio, avatar) "
                       "VALUES (?, ?, ?, ?, ?)",
                       ["oracle-demo", Decimal("1999.99"),
                        __import__("datetime").datetime(2026, 11, 12, 8, 30),
                        "CLOB 文本 🦄", b"\x00\x01\x02oracle"])
    show("影响行数", ins.update_count)
    keys = ins.generated_keys
    col_label = keys.columns[0].label
    show("generatedKeys 列", f"{col_label} (sqlType={keys.columns[0].sql_type})")

    if col_label.upper() == "ROWID":
        # Oracle 特有:ojdbc 返回 ROWID,用它回查真实自增值
        rowid = keys.first_row()[0]
        got = conn.query("SELECT id, name, price FROM JP_EXAMPLE WHERE rowid = ?",
                         [rowid]).dict_rows()[0]
        show("ROWID 回查", got)
        show("自增 id(整型)", got["ID"])

    banner("4. 读回验证(类型转换)")
    row = conn.query("SELECT name, price, created, bio, avatar FROM JP_EXAMPLE").dict_rows()[0]
    for k in ("NAME", "PRICE", "CREATED", "BIO", "AVATAR"):
        show(k, repr(row[k]))

    banner("5. 事务")
    with conn.transaction():
        conn.execute("INSERT INTO JP_EXAMPLE (name) VALUES (?)", ["in-txn"])
        show("事务内可见", conn.query("SELECT COUNT(*) FROM JP_EXAMPLE").scalar())
    show("提交后 count", conn.query("SELECT COUNT(*) FROM JP_EXAMPLE").scalar())

    cleanup_silently(conn, "DROP TABLE JP_EXAMPLE PURGE")
    conn.close()

    banner("6. 显式方言声明(多兼容模式数据库的推荐写法)")
    # OceanBase 同一个 URL 前缀有 MySQL/Oracle 两种模式;OpenGauss 兼容 PG。
    # URL 无法区分时,连接时声明方言:
    resp = client._t.request("POST", "/api/connections", {**ORACLE, "dialect": "oracle"})
    show("声明 dialect=oracle →", resp["dialect"])
    client._t.request("DELETE", f"/api/connections/{resp['connectionId']}")
    print("   可选值: mysql | mariadb | oceanbase_mysql | postgresql | pg |")
    print("           opengauss | gaussdb | oceanbase_pg | oracle | oceanbase_oracle")

    client.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
