package com.jdbcproxy;

import com.google.gson.JsonObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One logical session exposed to HTTP clients, backed by a small pool of
 * physical connections.
 *
 * <p>Lock ordering: {@code txnLock -> pool monitor}. Statements inside an
 * active transaction run on the pinned connection under {@code txnLock};
 * autocommit reads ({@code intent=read}) borrow from the pool and never
 * touch the transaction.</p>
 */
final class JdbcSession {
    final String id;
    final String url;
    final String user;
    final boolean readOnly;
    final String dialectHint;   // null = auto-detect
    final long createdAt = System.currentTimeMillis();

    private final ReentrantLock txnLock = new ReentrantLock();
    private volatile PhysicalPool pool;
    private volatile PooledConn pinnedConn;
    private volatile boolean closed;
    private volatile long lastUsedAt = System.currentTimeMillis();
    private volatile JsonObject dialectCache;
    private volatile String productLower;
    private volatile Integer isolationLevel;
    private volatile boolean alive = true;
    private volatile long lastValidatedAt = 0L;
    private volatile int keepaliveTimeout = 5;

    JdbcSession(String id, String url, String user, boolean readOnly, Integer isolationLevel,
                String dialectHint) {
        this.id = id;
        this.url = url;
        this.user = user;
        this.readOnly = readOnly;
        this.isolationLevel = isolationLevel;
        this.dialectHint = dialectHint;
    }

    void attachPool(PhysicalPool p) {
        this.pool = p;
    }

    PhysicalPool pool() {
        return pool;
    }

    ReentrantLock txnLock() {
        return txnLock;
    }

    PooledConn pinned() {
        return pinnedConn;
    }

    boolean inTransaction() {
        return !closed && pinnedConn != null;
    }

    boolean isClosed() {
        return closed;
    }

    Integer isolationLevel() {
        return isolationLevel;
    }

    void touch() {
        lastUsedAt = System.currentTimeMillis();
        alive = true;
    }

    long lastUsedAt() {
        return lastUsedAt;
    }

    void markDead() {
        alive = false;
    }

    void setKeepaliveTimeout(int seconds) {
        this.keepaliveTimeout = Math.max(1, seconds);
    }

    /**
     * Dialect-aware liveness probe: Oracle uses SELECT 1 FROM DUAL, everything
     * else SELECT 1. Used for borrow-time stale checks and background keepalive.
     */
    boolean validateConn(PooledConn pc) {
        try {
            String sql = "oracle".equals(dialectFamily()) ? "SELECT 1 FROM DUAL" : "SELECT 1";
            java.sql.Statement st = pc.conn.createStatement();
            try {
                st.setQueryTimeout(keepaliveTimeout);
                st.execute(sql);
            } finally {
                st.close();
            }
            alive = true;
            lastValidatedAt = System.currentTimeMillis();
            return true;
        } catch (SQLException e) {
            alive = false;
            return false;
        }
    }

    long lastValidatedAt() {
        return lastValidatedAt;
    }

    boolean isActive() {
        return alive;
    }

    JsonObject dialectCache() {
        return dialectCache;
    }

    void setDialectCache(JsonObject json) {
        this.dialectCache = json;
    }

    void initProductName(String product) {
        productLower = product == null ? "" : product.toLowerCase();
    }

    String productName() {
        String p = productLower;
        return p == null ? "" : p;
    }

    /** Client-declared hint, null for auto. */
    String dialectHint() {
        return dialectHint;
    }

    /** Coarse dialect family: mysql | postgresql | oracle | unknown. */
    String dialectFamily() {
        if (dialectHint != null) return Dialects.familyOf(dialectHint);
        return Dialects.familyOf(Dialects.infer(productName(), url.toLowerCase()));
    }

    /** The concrete declared/inferred dialect name (for the dialect snapshot). */
    String resolvedDialect() {
        if (dialectHint != null) return dialectHint;
        return Dialects.infer(productName(), url.toLowerCase());
    }

    /** True for PostgreSQL-family dialects (pg, opengauss, oceanbase in PG mode). */
    boolean isPgLike() {
        return "postgresql".equals(dialectFamily());
    }

    /** Applies session-level state to a physical connection (at creation / on change). */
    void applyState(Connection c) {
        try {
            c.setReadOnly(readOnly);
        } catch (SQLException ignored) {
        }
        Integer iso = isolationLevel;
        try {
            if (iso != null) c.setTransactionIsolation(iso);
        } catch (SQLException ignored) {
        }
    }

    // ---------------------------------------------------------------- transactions

    /** Pins a pool connection and turns autocommit off. */
    void beginTxn(Integer isolation) {
        txnLock.lock();
        try {
            if (closed) throw new ApiException(400, "connection is closed");
            if (pinnedConn != null) {
                throw new ApiException(409, "a transaction is already active on this connection (commit or rollback first)");
            }
            if (isolation != null && !isolation.equals(isolationLevel)) {
                changeIsolation(isolation);
            }
            PooledConn pc = pool.borrow(false, true);
            try {
                pc.conn.setAutoCommit(false);
            } catch (SQLException e) {
                pool.releaseSafely(pc);
                throw new ApiException(400, "failed to start transaction: " + e.getMessage(),
                        "sql_error", ApiException.sqlExtras(e));
            }
            pinnedConn = pc;
        } finally {
            txnLock.unlock();
        }
    }

    void commitTxn() {
        finishTxn(true);
    }

    void rollbackTxn() {
        finishTxn(false);
    }

    private void finishTxn(boolean commit) {
        txnLock.lock();
        try {
            PooledConn pc = pinnedConn;
            if (pc == null) {
                throw new ApiException(409, "no active transaction on this connection");
            }
            SQLException err = null;
            try {
                if (commit) {
                    pc.conn.commit();
                } else {
                    pc.conn.rollback();
                }
                pc.conn.setAutoCommit(true);
            } catch (SQLException e) {
                err = e;
            }
            pinnedConn = null;
            pool.releaseSafely(pc); // rolls back + restores autocommit if the driver left debris
            if (err != null) {
                throw new ApiException(400, err.getMessage(), "sql_error", ApiException.sqlExtras(err));
            }
        } finally {
            txnLock.unlock();
        }
    }

    void setAutoCommitTxn(boolean value) {
        if (value) {
            if (inTransaction()) commitTxn();
        } else {
            beginTxn(null);
        }
    }

    /** Changes the session-wide isolation level (applies to future + idle connections). */
    void changeIsolation(Integer lvl) {
        isolationLevel = lvl;
        if (lvl == null) return;
        PooledConn pc = pinnedConn;
        if (pc != null) {
            try {
                pc.conn.setTransactionIsolation(lvl);
            } catch (SQLException ignored) {
                // e.g. PostgreSQL forbids changing isolation mid-transaction
            }
        }
        if (pool != null) pool.applyStateToIdle();
    }

    /** Closes the session: wakes waiters, closes idle connections, rolls back any transaction. */
    void close() {
        txnLock.lock();
        try {
            if (closed) return;
            closed = true;
            pool.closeAll();
            PooledConn pc = pinnedConn;
            pinnedConn = null;
            if (pc != null) {
                try {
                    pc.conn.rollback(); // Connection.close() also rolls back, but be explicit
                } catch (SQLException ignored) {
                }
                pool.discard(pc);
            }
        } finally {
            txnLock.unlock();
        }
    }
}
