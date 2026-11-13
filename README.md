# jdbc-proxy

把任意 JDBC 驱动变成 HTTP 服务,让 **任何语言** 都能调用 `jars/` 里的数据库驱动 —— 无需为每种数据库单独装客户端。

```
┌─────────┐   ┌──────────────┐   JDBC   ┌────────────┐
│ Python  │   │ Java (Spring)│ ───────► │  MySQL     │
│ Go      │──►│  jdbc-proxy  │ ───────► │  Postgres  │
│ Node.js │   │  HTTP server │ ───────► │  Oracle …  │
└─────────┘   └──────────────┘          └────────────┘
```

仓库包含两部分:

| 目录 | 内容 |
|------|------|
| `server/` | Java 实现的 HTTP 网关 (`jdbc-proxy-server.jar`),从 `jars/` 目录动态加载 JDBC 驱动 |
| `client/` | Python 客户端库 `jdbc_proxy`(纯标准库,零依赖,含同步 + asyncio 客户端) |
| `examples/` | 9 个可独立运行的分主题示例(快速上手/类型/事务/批量/元数据/并发/异步/错误处理) |
| `jars/` | JDBC 驱动 jar:MySQL、PostgreSQL、Oracle、OpenGauss、OceanBase |
| `docker-compose.yaml` | 测试数据库(MySQL / PostgreSQL 默认启动;OpenGauss / Oracle 在 profile 中) |

---

## 1. 启动 Server

```bash
# 构建(需要 JDK 17+ 与 Maven)
cd server && mvn clean package

# 运行(默认监听 127.0.0.1:8089,自动扫描 ./jars 目录)
java -jar server/target/jdbc-proxy-server.jar --drivers-dir jars

# 可选参数
java -jar server/target/jdbc-proxy-server.jar \
  --host 0.0.0.0 \            # 对外暴露(默认 127.0.0.1)
  --port 8089 \
  --drivers-dir jars \        # 驱动 jar 目录,启动时扫描 *.jar
  --api-key my-secret \       # 要求 X-Api-Key 或 Authorization: Bearer
  --max-connections 64 \      # 最大逻辑 session 数
  --max-rows 10000 \          # 单次响应行数硬上限
  --idle-timeout 1800 \       # 空闲 session 自动回收(秒)
  --pool-size 8 \             # 每个 session 的物理连接池大小
  --reserve-reads 2 \         # 事务永不占用的连接数(读始终有余量)
  --max-physical 256 \        # 全局物理连接上限
  --max-batch 10000 \         # 批量执行单次行数上限
  --keepalive-interval 300 \  # 空闲连接活性探测阈值(秒,0=关闭)
  --keepalive-timeout 5 \     # 单次探测超时(秒)
  --threads 0                 # 0=虚拟线程(默认); N=固定平台线程池
```

对应环境变量:`JDBC_PROXY_PORT`、`JDBC_PROXY_HOST`、`JDBC_PROXY_DRIVERS_DIR`、`JDBC_PROXY_API_KEY`、`JDBC_PROXY_MAX_ROWS` 等。要求 JDK 21+(默认使用虚拟线程)。

启动后即加载 `jars/` 下全部驱动(通过 `META-INF/services/java.sql.Driver` 发现),本仓库默认加载:

```
com.mysql.cj.jdbc.Driver  com.oceanbase.jdbc.Driver  oracle.jdbc.OracleDriver
org.opengauss.Driver      org.postgresql.Driver
```

新增数据库只需把驱动 jar 丢进 `jars/` 目录并重启。

### 安全提示

这是一个"任意 SQL 执行"的网关,默认只监听 `127.0.0.1`。对外部署时务必:绑定内网地址 / 加 `--api-key` / 自行在前置网关做访问控制。所有响应中的 JDBC URL 均已脱敏(内嵌凭证与 `password=` 参数不会回显);`/health` 免鉴权仅供探针使用。

---

## 2. 并发模型(v1.1)

每个逻辑 session 背后是一个**物理连接池**(默认 8 条),由服务端调度:

```
客户端线程 A ── intent=read ──► [读队列,优先] ──► 空闲连接 #3 ─┐
客户端线程 B ── intent=read ──►                               ├─ 并行执行
客户端线程 C ── intent=read ──►                               ┘
客户端线程 D ── transaction begin ──► 钉住连接 #1(独占,直至 commit/rollback)
```

| 规则 | 说明 |
|------|------|
| 独占借用 | 借出的物理连接归一个请求线程独占,满足 JDBC 线程安全要求 |
| 读优先 | 连接竞争时,读队列先于写队列被调度 |
| 读预留 | 事务最多钉住 `pool-size - reserve-reads` 条连接,读永远有余量 |
| intent=read | `execute` 声明 `intent:"read"` 时强制 autocommit + 读队列,**永不进入事务、不被写阻塞**;只读失败(连接死)自动换新连接重试一次 |
| 事务 | `begin` 钉住一条连接;同一 session 的写语句加入该事务;commit/rollback 释放 |
| 空闲回收 | `idle-timeout` 到期的 session 被回收(不会打断进行中的语句/事务) |

**对客户端的意义**(Python 多线程 / asyncio / Go goroutine / Java 并发流,直接打 HTTP 即可):

- 同一连接对象上并发 `query()` → 真并行(8 线程 8 条物理连接)
- 长事务只影响开启事务的那个线程,其他线程的读不受影响,且看不到未提交数据
- Python 客户端用 contextvar 跟踪"当前线程是否在事务中":事务内的 `query()` 自动加入事务(能看到自己的未提交修改),事务外的 `query()` 自动声明 `intent=read` 走读优先通道

性能设计:每条物理连接带 LRU PreparedStatement 缓存(32 条,PG 免重复 parse);HTTP 层默认虚拟线程;同步客户端按线程复用 keep-alive 连接;异步客户端内置 keep-alive 连接池。

---

## 2.1 连接上限、保活与故障恢复

### 上限控制(服务端启动配置)

| 配置 | 默认 | 作用 |
|------|------|------|
| `--max-connections` | 64 | **逻辑 session 上限**,超出返回 503(错误信息带解决指引) |
| `--max-physical` | 256 | **全局物理连接上限**(所有 session 的连接池总和),超出时 borrow 等待直至超时 503 |
| `--pool-size` / `--reserve-reads` | 8 / 2 | 每 session 池大小 / 事务不可占用的读预留 |

### 定时清理与活性探测

| 配置 | 默认 | 作用 |
|------|------|------|
| `--idle-timeout` | 1800s | 空闲 session 定时回收(30s 周期扫描;进行中的语句/事务不打断) |
| `--keepalive-interval` | 300s(0=关) | 双重作用:① 后台定时探测**空闲超过该阈值**的物理连接,死连接立即从池中清除;② 借出阈值——空闲超过该阈值的连接在**交给任何请求前先探测** |
| `--keepalive-timeout` | 5s | 单次探测语句超时 |

探测语句按方言选择:Oracle → `SELECT 1 FROM DUAL`,其他 → `SELECT 1`。因此数据库侧超时(wait_timeout)、防火墙切断、网络故障产生的死连接,最多在 keepalive-interval 内被清理,或者在下一次交互时被借出校验拦下——**客户端永远拿不到死连接**。

### 失败重连重发

| 场景 | 行为 |
|------|------|
| 借出校验发现死连接 | 丢弃并换池内其他连接/重建,**对调用方完全透明**(含写语句——语句尚未执行,无双写风险) |
| 读语句执行中遇连接故障 | 丢弃死连接,自动换新连接**重发一次**(SELECT 幂等) |
| 写语句执行中遇连接故障 | 不自动重发(无法确认是否已提交,避免双写);错误为结构化 SQL 错误,由调用方决定重试 |
| 事务内语句 | 不重试(事务状态绑定在特定连接上) |

连接的活性信息对所有语言可见:`POST /api/connections` 响应、`GET /api/connections` 列表、`GET /api/connections/{id}` 均含 `active`(最近交互/探测是否正常)与 `lastValidatedAt`(最近一次成功探测时间)。

---

## 2. HTTP 协议

所有请求/响应均为 JSON(`Content-Type: application/json`)。错误响应统一为:

```json
{"error": {"type": "sql_error", "message": "...", "status": 400,
           "sqlState": "42S02", "errorCode": 1146, "causedBy": "...(可选)"}}
```

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 存活检查、版本、连接数、maxRows |
| GET | `/drivers` | 已加载驱动列表 |
| POST | `/api/connections` | 建立连接(见下) |
| GET | `/api/connections` | 列出打开的连接 |
| GET | `/api/connections/{id}` | 连接详情 |
| DELETE | `/api/connections/{id}` | 关闭连接 |
| POST | `/api/connections/{id}/execute` | 执行单条 SQL(查询或更新) |
| POST | `/api/connections/{id}/batch` | 批量执行 |
| POST | `/api/connections/{id}/transaction` | begin / commit / rollback / setAutoCommit / setIsolation |
| GET | `/api/connections/{id}/metadata/dialect` | 数据库方言快照 |
| GET | `/api/connections/{id}/metadata/tables` | 表/视图列表 |
| GET | `/api/connections/{id}/metadata/columns` | 列结构 |
| GET | `/api/connections/{id}/metadata/primaryKeys` | 主键 |
| GET | `/api/connections/{id}/metadata/foreignKeys` | 外键(`direction=imported|exported`) |
| GET | `/api/connections/{id}/metadata/indexes` | 索引 |
| GET | `/api/connections/{id}/metadata/schemas` / `catalogs` / `tableTypes` / `typeInfo` | 方言级元数据 |

### 建立连接

```json
POST /api/connections
{
  "url": "jdbc:mysql://127.0.0.1:3306/default_db",
  "user": "root", "password": "***",
  "driverClass": "com.mysql.cj.jdbc.Driver",   // 可省略,按 URL 前缀自动选择
  "properties": {"useSSL": "false"},            // 额外驱动属性(可选)
  "autoCommit": true, "readOnly": false,
  "isolation": "READ_COMMITTED",                // 可选:名称或 JDBC 整数级
  "dialect": "mysql"                            // 可选:显式方言声明(见下)
}
→ 201 {"connectionId": "feab54d81abc446e", "dialect": "mysql", ...}
```

### 方言与兼容模式数据库(重要)

OceanBase 有 MySQL / Oracle 两种模式、OpenGauss 兼容 PostgreSQL/Oracle —— **同一个 URL 前缀无法区分真实方言**,而方言决定了参数绑定(jsonb 的 Types.OTHER)、catalog/schema 语义、ping 语句等行为。因此:

- **默认(auto)**:按驱动上报的 productName + URL 前缀推断,常规数据库(MySQL/PostgreSQL/Oracle/原版驱动)无需声明;
- **显式声明(推荐给兼容模式数据库)**:连接时传 `"dialect"`,服务端一切方言相关行为按声明执行,`/metadata/dialect` 响应中 `dialect`(解析出的 family: mysql/postgresql/oracle)与 `dialectHint`(声明原文)回显:

| dialect 取值 | family | 适用 |
|---|---|---|
| `mysql` / `mariadb` / `oceanbase_mysql` | mysql | MySQL、MariaDB、OceanBase MySQL 模式 |
| `postgresql` / `pg` / `opengauss` / `gaussdb` / `oceanbase_pg` | postgresql | PostgreSQL、OpenGauss、OceanBase PG 模式 |
| `oracle` / `oceanbase_oracle` | oracle | Oracle、OceanBase Oracle 模式 |

未声明且无法推断(如 `jdbc:oceanbase://`)时 family=unknown,行为最保守;声明未知值返回 400 并列出可选项。

### 执行 SQL

```json
POST /api/connections/{id}/execute
{
  "sql": "SELECT * FROM EMP WHERE DEPTNO = ?",
  "params": [20],
  "maxRows": 1000,          // 可选,受服务端硬上限约束
  "fetchSize": 500,         // 可选
  "timeoutSeconds": 30      // 可选
}
```

查询返回(`truncated=true` 表示超过 maxRows 还有更多行):

```json
{"statementType": "query",
 "columns": [{"label":"SAL","name":"SAL","sqlType":3,"typeName":"DECIMAL",
              "precision":7,"scale":2,"nullable":1,"autoIncrement":false,...}],
 "rows": [["7369","SMITH","800.00","1980-12-17"]],
 "rowCount": 1, "truncated": false, "updateCount": null, "generatedKeys": null}
```

更新返回 `{"statementType":"update","updateCount":1,"generatedKeys":{...}|null}`。
PostgreSQL `INSERT ... RETURNING` 的返回行由驱动放进 `generatedKeys`。

**值编码规则**(保证跨语言无损):

- 整数 `TINYINT..BIGINT` → JSON 数字;超过 64 位(如 MySQL `BIGINT UNSIGNED` 溢出)→ 字符串
- `DECIMAL/NUMERIC` → **字符串**(`"800.00"`),避免 JSON 浮点丢精度
- `DATE/TIME/TIMESTAMP` → ISO-8601 字符串(`2024-01-01`、`12:34:56.123456`、带时区为 `2024-01-01T12:00:00+08:00`)
- 二进制/BLOB → base64 字符串;CLOB → 字符串
- `BIT(>1)` → base64;`BOOLEAN/BIT(1)` → true/false

**参数类型提示**(严格方言需要,如 PostgreSQL 拒绝 varchar → numeric/date/jsonb 隐式转换):

```json
{"sql": "INSERT INTO t (price, day, meta) VALUES (?, ?, ?)",
 "params": [{"type": "decimal", "value": "12.50"},
            {"type": "date", "value": "2026-01-15"},
            {"type": "json", "value": "{\"a\":1}"}]}
```

支持 type:`decimal` `int` `float` `bool` `date` `time` `timestamp` `json` `bytes`(base64)`text`。Python 客户端会自动把 `Decimal/date/time/datetime/bytes` 包装成对应提示,一般无需手写。

### 批量执行

```json
POST /api/connections/{id}/batch
{"sql": "INSERT INTO students (name, age) VALUES (?, ?)",
 "batch": [["a", 1], ["b", 2]]}
→ {"statementType": "batch", "updateCounts": [1, 1], "generatedKeys": {...}}
```

部分失败时(`BatchUpdateException`)HTTP 仍返回错误,但 error 里带 `updateCounts` 便于核对已提交行数。

### 事务

```json
POST /api/connections/{id}/transaction  {"action": "begin", "isolation": "REPEATABLE_READ"}
POST /api/connections/{id}/transaction  {"action": "commit"}
POST /api/connections/{id}/transaction  {"action": "rollback"}
```

`begin` 将 autoCommit 置为 false;`commit/rollback` 成功后恢复 autoCommit=true。

### 方言元数据示例

`GET /api/connections/{id}/metadata/dialect` 返回驱动 `DatabaseMetaData` 的关键信息,供调用方做方言适配:

```json
{"productName": "MySQL", "identifierQuoteString": "`", "catalogTerm": "database", "schemaTerm": "",
 "storesLowerCaseIdentifiers": false, "supportsGetGeneratedKeys": true,
 "defaultTransactionIsolation": 4, "jdbcUrl": "jdbc:mysql://...", "userName": "root", ...}
```

元数据查询的查询参数与 JDBC `DatabaseMetaData` 一一对应:`catalog`、`schema`/`schemaPattern`、`table`/`tablePattern`、`columnPattern`、`types`(逗号分隔,如 `TABLE,VIEW`)。注意 MySQL 的"库"是 catalog(`catalog=default_db`),PostgreSQL 的"模式"是 schema(`schemaPattern=public`)。

响应统一为 `{columns, rows, rowCount, truncated}` —— 与 execute 相同的形状。

---

## 3. Python 客户端

```bash
pip install ./client          # 或直接把 client/ 加入 PYTHONPATH
```

```python
from decimal import Decimal
from datetime import date
from jdbc_proxy import JdbcProxyClient, Typed

with JdbcProxyClient("http://127.0.0.1:8089") as client:
    # 连接(同时支持 mysql/postgresql/oracle/opengauss/oceanbase 的 JDBC URL)
    with client.connect(
        url="jdbc:mysql://127.0.0.1:3306/default_db",
        user="root", password="***",
        # properties={"useSSL": "false"}, isolation="READ_COMMITTED", read_only=False
    ) as conn:

        # 查询:参数化 + 自动类型转换(DECIMAL→Decimal, DATE→date, TIMESTAMP→datetime)
        r = conn.query("SELECT * FROM EMP WHERE deptno > ?", [10])
        r.column_names          # ['EMPNO', 'ENAME', ...]
        r.rows                  # [(7369, 'SMITH', ...), ...] 元组列表
        r.dict_rows()           # [{'EMPNO': 7369, ...}, ...]
        r.scalar()              # 单行单列快捷方式
        r.truncated             # 是否被 maxRows 截断

        # 执行:影响行数 + 自增主键
        res = conn.execute("INSERT INTO students (name, age) VALUES (?, ?)", ["Tom", 20])
        res.update_count            # 1
        res.generated_keys.first_row()   # (11,)

        # 批量执行
        batch = conn.execute_many(
            "INSERT INTO students (name, age) VALUES (?, ?)",
            [["a", 1], ["b", 2], ["c", 3]])
        batch.update_counts         # [1, 1, 1]
        batch.generated_keys        # 自增键(若有)

        # 事务(异常自动回滚)
        with conn.transaction(isolation="REPEATABLE_READ"):
            conn.execute("UPDATE accounts SET ...")

        # 方言感知的元数据:MySQL 自动按 URL 的库查 catalog,PG 默认 schema=public
        conn.list_tables()                          # [{catalog, schema, name, type, remarks}]
        conn.list_tables(table="t%", schema=None)   # 显式指定
        conn.describe_table("EMP")                  # 列结构(名称/类型/精度/可空/自增/注释)
        conn.get_primary_keys("EMP")
        conn.get_foreign_keys("EMP")                # direction="exported" 反向查询
        conn.get_indexes("EMP")
        conn.get_schemas() / conn.get_catalogs() / conn.get_table_types()
        conn.get_type_info()                        # 方言类型清单
        conn.dialect()                              # 产品名/引号符/隔离级/限制等

        # PostgreSQL 专用:JSONB 列用 Typed 提示
        conn.execute('INSERT INTO t (meta) VALUES (?)', Typed("json", {"a": 1}))

        conn.ping()             # SELECT 1 探活
# 退出 with 块自动 close 连接
```

**并发 / 异步**(v1.1):同一连接对象可被多线程 / 协程共享 —— 并发 `query()` 在服务端是真并行(读优先);事务通过 contextvar 绑定到"当前线程/任务":

```python
import asyncio
from jdbc_proxy import AsyncJdbcProxyClient   # 纯 asyncio 实现,零第三方依赖

# 多线程:并发读 + 事务隔离
with ThreadPoolExecutor(8) as pool:
    rows = list(pool.map(lambda i: conn.query("SELECT ... WHERE id > ?", [i]).dict_rows(), range(8)))

def writer():
    with conn.transaction():                  # 钉住一条物理连接
        conn.execute("UPDATE ...")

# asyncio
async with AsyncJdbcProxyClient("http://127.0.0.1:8089") as client:
    conn = await client.connect(url="jdbc:postgresql://...", user="u", password="p")
    results = await asyncio.gather(*[conn.query("SELECT ...") for _ in range(8)])  # 真并行
    async with conn.transaction(isolation="REPEATABLE_READ"):
        await conn.execute("INSERT ...")
    await conn.close()
```

错误处理:所有失败抛 `jdbc_proxy.JdbcProxyError`,携带 `message / status / sql_state / error_code / caused_by`。

其他语言接同一套 HTTP 协议即可(见第 2 节),客户端只是协议的便捷封装。

---

## 4. 测试

```bash
# 启动数据库 + 代理
docker compose up -d mysql postgres
java -jar server/target/jdbc-proxy-server.jar --drivers-dir jars &

# Java 单元测试(38 个:配置解析、URL 脱敏、值编码纯函数、连接错误识别)
cd server && mvn test

# Python 测试(140 个)
cd client && python3 -m unittest discover -s tests -v
#   tests/test_units.py        41  纯单元:参数归一化/类型转换/结果塑形/方言默认(无需 DB)
#   tests/test_integration.py  31  功能回归:两库核心流程
#   tests/test_functional.py   51  值编解码边界/事务语义/元数据/错误协议/脱敏/客户端行为
#   tests/test_concurrency.py  11  并发:并行读、读优先、事务隔离、asyncio、计数守恒
#   tests/test_server_flags.py  6  独立实例验证 --api-key 401/200 与 --max-connections 503

# 完整示例(9 个,见 examples/README.md)
python3 examples/demo.py
```

环境变量可覆盖默认地址:`JDBC_PROXY_URL`、`MYSQL_URL/USER/PASSWORD`、`PG_URL/USER/PASSWORD`。

## 5. 已验证能力

- ✅ MySQL 8.4 / PostgreSQL 15 参数化查询、类型转换(DECIMAL 精度无损、DATE/TIMESTAMP→Python 原生)
- ✅ 自增主键返回(MySQL `getGeneratedKeys`)与 `INSERT ... RETURNING`(PostgreSQL)
- ✅ 服务端批量执行 + 部分失败报告
- ✅ 事务 begin/commit/rollback + 隔离级别
- ✅ 方言元数据:表/列/主键/外键/索引/schema/catalog/类型清单
- ✅ 结构化 SQL 错误(sqlState、vendor code)、连接生命周期管理、空闲回收
- ✅ **并发**:8 线程/协程同连接并行读(已验证多物理连接)、读优先调度、事务不阻塞读、跨连接未提交数据隔离、物理连接计数守恒(回归覆盖)
- ✅ **异步**:asyncio 客户端 gather 并行 + 事务上下文(纯标准库)
- ✅ Oracle 23ai:方言自动识别/显式声明、IDENTITY 自增(ROWID 两步取键)、NUMBER/DATE/TIMESTAMP(TZ)/CLOB/BLOB 类型往返、Oracle 元数据(schema 语义)
- ✅ 方言声明机制:连接时 `dialect=` 显式声明,覆盖 OceanBase(MySQL/Oracle 模式)、OpenGauss 等兼容模式数据库的区分(单测覆盖)
- ✅ Oracle 之外的驱动(MySQL/PG/Oracle/OpenGauss/OceanBase)已全部加载,按 `docker-compose.yaml` 的 profile 启动对应容器即可测试
