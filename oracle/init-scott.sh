#!/bin/bash
# 在 gvenzl/oracle-free 容器首次初始化时执行(以 SYS 身份运行)。
# 创建 SCOTT 测试用户与示例表。
echo "[init-scott] creating SCOTT test user and seed tables..."
sqlplus -s / as sysdba @/initdb/scott_seed.sql
echo "[init-scott] done."
