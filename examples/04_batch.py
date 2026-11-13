#!/usr/bin/env python3
"""04 — 批量执行:服务端 addBatch、自增键、部分失败报告。

运行:  python3 examples/04_batch.py
前提:  MySQL 可用。
"""

from _common import MYSQL, banner, open_client, show


def main() -> None:
    client = open_client()

    with client.connect(**MYSQL) as conn:
        conn.execute(
            "DROP TABLE IF EXISTS jp_batch_demo")
        conn.execute(
            "CREATE TABLE jp_batch_demo ("
            "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20) UNIQUE, score INT)")
        conn.execute("DELETE FROM jp_batch_demo")

        banner("1. 批量插入 + 批量自增键")
        rows = [(f"player-{i}", 60 + i) for i in range(1, 6)]
        b = conn.execute_many(
            "INSERT INTO jp_batch_demo (name, score) VALUES (?, ?)", rows)
        show("每行影响数", b.update_counts)            # [1, 1, 1, 1, 1]
        show("自增键", b.generated_keys.dict_rows())   # 5 个新 id

        banner("2. 批量更新")
        b2 = conn.execute_many(
            "UPDATE jp_batch_demo SET score = score + ? WHERE name = ?",
            [[100, "player-1"], [10, "player-2"]])
        show("updateCounts", b2.update_counts)

        banner("3. 部分失败:错误位置之前的行仍会执行")
        # name 有 UNIQUE 约束:第二行与第一行重复 → executeBatch 抛错,
        # error 里带回已完成行的计数(updateCounts)
        try:
            conn.execute_many(
                "INSERT INTO jp_batch_demo (name, score) VALUES (?, ?)",
                [["winner", 1], ["winner", 2], ["never-reached", 3]])
        except Exception as e:
            show("错误", f"{type(e).__name__}: {str(e)[:80]}")
            show("已完成行计数(error 载荷)", e.raw_error.get("updateCounts"))

        show("最终行数", conn.query("SELECT COUNT(*) FROM jp_batch_demo").scalar())
        conn.execute("DROP TABLE jp_batch_demo")
    client.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
