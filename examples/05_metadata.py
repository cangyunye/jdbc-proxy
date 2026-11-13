#!/usr/bin/env python3
"""05 — 元数据:方言信息、表结构、主键外键索引、schema/catalog。

运行:  python3 examples/05_metadata.py
前提:  MySQL + PostgreSQL 可用。
"""

from _common import MYSQL, PG, banner, open_client, show


def mysql_demo(client) -> None:
    with client.connect(**MYSQL) as conn:
        banner("1. 方言快照(dialect)—— 做跨库工具的关键信息")
        d = conn.dialect()
        for k in ("productName", "productVersion", "identifierQuoteString",
                  "catalogTerm", "schemaTerm", "storesLowerCaseIdentifiers",
                  "supportsGetGeneratedKeys", "defaultTransactionIsolation"):
            show(k, d.get(k))

        banner("2. 表列表(方言感知默认:MySQL 用 URL 里的库当 catalog)")
        for t in conn.list_tables(table="st%"):
            show("table", f"{t['name']}  [{t['type']}]")

        banner("3. 表结构 describe_table")
        for c in conn.describe_table("students"):
            show("column", f"{c['name']:<8} {c['type']:<10} "
                           f"nullable={c['nullable']} auto_inc={c['auto_increment']} "
                           f"precision={c['precision']}")

        banner("4. 主键 / 索引")
        show("students 主键", conn.get_primary_keys("students"))
        for idx in conn.get_indexes("students")[:3]:
            show("index", f"{idx['index_name']} on {idx['column']} unique={not idx['non_unique']}")

        banner("5. 外键(建一对示例表)")
        conn.execute("DROP TABLE IF EXISTS jp_md_c")
        conn.execute("DROP TABLE IF EXISTS jp_md_p")
        conn.execute("CREATE TABLE jp_md_p (id INT PRIMARY KEY, code VARCHAR(8))")
        conn.execute(
            "CREATE TABLE jp_md_c (id INT PRIMARY KEY, pid INT NOT NULL,"
            " FOREIGN KEY (pid) REFERENCES jp_md_p (id))")
        for fk in conn.get_foreign_keys("jp_md_c"):
            show("imported FK", f"{fk['fk_table']}.{fk['fk_column']} -> {fk['pk_table']}.{fk['pk_column']}")
        for fk in conn.get_foreign_keys("jp_md_p", direction="exported"):
            show("exported FK", f"{fk['fk_table']}.{fk['fk_column']} <- {fk['pk_table']}.{fk['pk_column']}")
        conn.execute("DROP TABLE jp_md_c")
        conn.execute("DROP TABLE jp_md_p")

        banner("6. catalog / 表类型 / 方言类型清单")
        show("catalogs(即 MySQL 的库)", conn.get_catalogs()[:6])
        show("table types", conn.get_table_types())
        show("typeInfo 条数(截断演示)", len(conn.get_type_info(max_rows=8)))


def pg_demo(client) -> None:
    with client.connect(**PG) as conn:
        banner("7. PostgreSQL:schema 世界")
        show("schemas", [s["schema"] for s in conn.get_schemas()][:8])
        show("public 下的表", [t["name"] for t in conn.list_tables(schema="public")])
        show("EMP 列", [(c["name"], c["type"]) for c in conn.describe_table("EMP")])


def main() -> None:
    client = open_client()
    mysql_demo(client)
    pg_demo(client)
    client.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
