#!/usr/bin/env python3
"""06 — 并发:多线程共享一个连接,读真并行、读优先、事务不阻塞读。

运行:  python3 examples/06_concurrency.py
前提:  MySQL 可用。

关键点:
  * 服务端为每个逻辑连接维护一个小型物理连接池(默认 8 条);
  * query() 自动声明 intent=read → 并行执行、读优先调度、永不进入事务;
  * 事务钉住其中 1 条物理连接,只影响发起事务的那个线程。
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from _common import MYSQL, banner, open_client, show


def main() -> None:
    client = open_client()

    with client.connect(**MYSQL) as conn:
        banner("1. 8 个线程并行查询同一个连接")
        with ThreadPoolExecutor(max_workers=8) as pool:
            t0 = time.perf_counter()
            # SLEEP(0.3) 让耗时可见:如果服务端串行执行,总耗时 >= 2.4s
            results = list(pool.map(
                lambda i: conn.query("SELECT SLEEP(0.3), CONNECTION_ID()").first_row()[1],
                range(8)))
            elapsed = time.perf_counter() - t0
        show("总耗时", f"{elapsed:.2f}s(串行约 2.4s+)")
        show("用到的物理连接数", len(set(results)))
        print("   -> 同一个逻辑连接的并发读,在服务端是真正并行的")

        banner("2. 事务不阻塞其他线程的读")
        started = threading.Event()

        def long_txn():
            conn.begin()  # 钉住一条物理连接(只影响本线程)
            started.set()
            try:
                conn.execute("SELECT SLEEP(1.0)")  # 长语句在事务连接上执行
            finally:
                conn.rollback()

        t = threading.Thread(target=long_txn)
        t.start()
        started.wait(5)
        time.sleep(0.2)  # 等长语句开始

        t0 = time.perf_counter()
        n = conn.query("SELECT COUNT(*) FROM students").scalar()  # 其他连接,立即返回
        read_elapsed = time.perf_counter() - t0
        t.join()
        show("事务中的长语句", "SLEEP(1.0)")
        show("并行读耗时", f"{read_elapsed*1000:.0f}ms")
        show("读到的行数", n)
        print("   -> 读走独立的物理连接,不等事务")

        banner("3. 事务的可见性边界")
        conn.execute("DROP TABLE IF EXISTS jp_conc_demo")
        conn.execute("CREATE TABLE jp_conc_demo (id INT PRIMARY KEY)")
        conn.execute("DELETE FROM jp_conc_demo")
        conn.begin()
        conn.execute("INSERT INTO jp_conc_demo VALUES (999)")
        visible_to_txn_thread = threading.Event()

        def parallel_reader():
            # 本线程不在事务里 → intent=read → 独立连接 → 看不到未提交行
            v = conn.query("SELECT COUNT(*) FROM jp_conc_demo WHERE id = 999").scalar()
            visible_to_txn_thread.set()
            show("并行读看到的未提交行", v)  # 0
        r = threading.Thread(target=parallel_reader)
        r.start()
        visible_to_txn_thread.wait(5)
        r.join()
        conn.rollback()
        conn.execute("DROP TABLE jp_conc_demo")

        banner("4. 用连接池对象并发打开多个连接")
        # 也可以完全并行地开多条逻辑连接(HTTP keep-alive 复用)
        with ThreadPoolExecutor(max_workers=4) as pool:
            ids = list(pool.map(lambda _: client.connect(**MYSQL).id, range(4)))
        show("新连接", ids)
        for cid in ids:
            client._t.request("DELETE", f"/api/connections/{cid}")

    client.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
