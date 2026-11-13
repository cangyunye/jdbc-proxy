# jdbc-proxy 示例集

每个示例都是**可独立运行**的脚本,演示一个主题。全部纯标准库,无第三方依赖。

## 前提

```bash
# 1. 启动数据库(MySQL/PostgreSQL 默认启动;Oracle 首次初始化需几分钟)
docker compose up -d mysql postgres
docker compose up -d oracle          # 仅 09_oracle.py 需要

# 2. 构建并启动代理
cd server && mvn clean package && cd ..
java -jar server/target/jdbc-proxy-server.jar --drivers-dir jars
```

## 运行

```bash
python3 examples/01_quickstart.py          # 从仓库根目录
# 或
cd examples && python3 01_quickstart.py    # 从 examples 目录
```

## 示例索引

| 脚本 | 主题 | 依赖的库 |
|------|------|----------|
| [01_quickstart.py](01_quickstart.py) | 最小流程:连接 → 参数化查询 → 写入取自增键 → ping | MySQL |
| [02_types_and_params.py](02_types_and_params.py) | 原生类型直传(Decimal/date/time/datetime/bytes/bool/中文 emoji)、Typed 提示(JSONB、timestamptz、显式 decimal) | MySQL + PostgreSQL |
| [03_transactions.py](03_transactions.py) | 事务:提交/回滚/隔离级别/事务内可见性/低层 set_auto_commit | MySQL |
| [04_batch.py](04_batch.py) | 服务端批量:批量自增键、批量更新、部分失败报告 | MySQL |
| [05_metadata.py](05_metadata.py) | 方言快照、表结构、主键/外键/索引、schema/catalog、类型清单 | MySQL + PostgreSQL |
| [06_concurrency.py](06_concurrency.py) | 多线程共享连接:并行读、读优先、事务不阻塞读、可见性边界 | MySQL |
| [07_async.py](07_async.py) | asyncio:AsyncJdbcProxyClient、gather 并行、异步事务 | MySQL + PostgreSQL |
| [08_errors.py](08_errors.py) | 结构化 SQL 错误(sqlState/code)、连接生命周期、常见陷阱 | MySQL |
| [09_oracle.py](09_oracle.py) | Oracle:方言自动识别/显式声明、IDENTITY 自增(ROWID 两步取键)、全类型往返 | Oracle |
| [demo.py](demo.py) | 全功能一页式演示(早期总览版) | MySQL + PostgreSQL |

## 可用环境变量

| 变量 | 默认值 |
|------|--------|
| `JDBC_PROXY_URL` | `http://127.0.0.1:8089` |
| `MYSQL_URL` / `MYSQL_USER` / `MYSQL_PASSWORD` | `jdbc:mysql://127.0.0.1:3306/default_db` / `root` / `root123456` |
| `PG_URL` / `PG_USER` / `PG_PASSWORD` | `jdbc:postgresql://127.0.0.1:5432/postgres_db` / `postgres` / `postgres123` |
| `ORACLE_URL` / `ORACLE_USER` / `ORACLE_PASSWORD` | `jdbc:oracle:thin:@//127.0.0.1:1521/XEPDB1` / `scott` / `tiger` |

示例会创建 `jp_*` 前缀的临时表并在结束时删除;中途 Ctrl+C 最多留下残余表,不影响其他示例。

## 一分钟用法速查

```python
from jdbc_proxy import JdbcProxyClient, Typed

with JdbcProxyClient("http://127.0.0.1:8089") as client:
    with client.connect(url="jdbc:mysql://127.0.0.1:3306/default_db",
                        user="root", password="***") as conn:
        rows = conn.query("SELECT * FROM EMP WHERE deptno > ?", [10]).dict_rows()
        res = conn.execute("INSERT INTO students (name) VALUES (?)", ["Tom"])
        new_id = res.generated_keys.first_row()[0]
        with conn.transaction(isolation="REPEATABLE_READ"):
            conn.execute("UPDATE ...")
        print(conn.describe_table("EMP"), conn.list_tables())
```
