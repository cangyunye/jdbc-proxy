#!/usr/bin/env python3
"""03 — 事务:commit / rollback / 隔离级别 / 事务内外的可见性。

运行:  python3 examples/03_transactions.py
前提:  MySQL 可用。
"""

from _common import MYSQL, banner, open_client, show


def main() -> None:
    client = open_client()

    with client.connect(**MYSQL) as conn:
        conn.execute(
            "DROP TABLE IF EXISTS jp_txn_demo")
        conn.execute(
            "CREATE TABLE jp_txn_demo (id INT PRIMARY KEY, name VARCHAR(20))")
        conn.execute("DELETE FROM jp_txn_demo")

        banner("1. 正常提交")
        with conn.transaction() as t:
            conn.execute("INSERT INTO jp_txn_demo VALUES (?, ?)", [1, "committed"])
        show("提交后行数", conn.query("SELECT COUNT(*) FROM jp_txn_demo").scalar())

        banner("2. 异常自动回滚")
        try:
            with conn.transaction():
                conn.execute("INSERT INTO jp_txn_demo VALUES (?, ?)", [2, "should-roll-back"])
                raise RuntimeError("模拟业务失败")
        except RuntimeError as e:
            show("捕获异常", e)
        show("回滚后行数", conn.query("SELECT COUNT(*) FROM jp_txn_demo").scalar())  # 仍是 1

        banner("3. 事务内能看到自己的未提交修改")
        with conn.transaction():
            conn.execute("INSERT INTO jp_txn_demo VALUES (?, ?)", [3, "in-txn"])
            inside = conn.query("SELECT COUNT(*) FROM jp_txn_demo").scalar()
            show("事务内 count", inside)          # 2(能看到刚插的未提交行)
        show("提交后 count", conn.query("SELECT COUNT(*) FROM jp_txn_demo").scalar())  # 2

        banner("4. 指定隔离级别 + 手动 begin/commit")
        conn.begin(isolation="REPEATABLE_READ")
        conn.execute("INSERT INTO jp_txn_demo VALUES (?, ?)", [4, "manual"])
        conn.commit()
        show("手动事务后 count", conn.query("SELECT COUNT(*) FROM jp_txn_demo").scalar())  # 3

        banner("5. 低层 API:set_auto_commit(等价于 begin/commit)")
        conn.set_auto_commit(False)   # 开启事务
        conn.execute("INSERT INTO jp_txn_demo VALUES (?, ?)", [5, "lowlevel"])
        conn.set_auto_commit(True)    # 提交
        show("count", conn.query("SELECT COUNT(*) FROM jp_txn_demo").scalar())  # 4

        conn.execute("DROP TABLE jp_txn_demo")
    client.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
