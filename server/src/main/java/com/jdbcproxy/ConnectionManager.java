package com.jdbcproxy;

import com.google.gson.JsonObject;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Registry of logical sessions (each backed by a physical connection pool) with an idle reaper. */
final class ConnectionManager {
    private final Config cfg;
    private final DriverRegistry drivers;
    private final ConcurrentHashMap<String, JdbcSession> sessions = new ConcurrentHashMap<>();
    private final Semaphore physicalPermits;
    private final AtomicInteger livePhysical = new AtomicInteger();
    private final ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "jdbc-proxy-reaper");
        t.setDaemon(true);
        return t;
    });

    ConnectionManager(Config cfg, DriverRegistry drivers) {
        this.cfg = cfg;
        this.drivers = drivers;
        this.physicalPermits = new Semaphore(cfg.maxPhysical());
        if (cfg.idleTimeoutSeconds() > 0) {
            reaper.scheduleAtFixedRate(this::reapIdle, 30, 30, TimeUnit.SECONDS);
        }
        if (cfg.keepaliveInterval() > 0) {
            long period = Math.max(1, Math.min(60, cfg.keepaliveInterval() / 2));
            reaper.scheduleAtFixedRate(this::keepAliveSweep, period, period, TimeUnit.SECONDS);
        }
    }

    JdbcSession create(String url, String user, String password, String driverClass,
                       Properties extraProps, boolean autoCommit, boolean readOnly, Integer isolation,
                       String dialectHint) {
        if (url == null || !url.regionMatches(true, 0, "jdbc:", 0, 5)) {
            throw new ApiException(400, "'url' must be a JDBC URL starting with jdbc:");
        }
        if (sessions.size() >= cfg.maxConnections()) {
            throw new ApiException(503, "connection limit reached: " + cfg.maxConnections()
                    + " sessions are open (server option --max-connections). "
                    + "Close sessions via DELETE /api/connections/{id} or raise the limit.");
        }
        String hint = Dialects.normalizeHint(dialectHint);

        Driver driver = drivers.find(url, driverClass);
        Properties props = extraProps != null ? (Properties) extraProps.clone() : new Properties();
        if (user != null) props.setProperty("user", user);
        if (password != null) props.setProperty("password", password);

        String id = UUID.randomUUID().toString().replace("-", "");
        JdbcSession session = new JdbcSession(id, url, user, readOnly, isolation, hint);
        PhysicalPool pool = new PhysicalPool(cfg.poolSize(), cfg.reserveReads(), cfg.borrowWaitMs(),
                physicalPermits, livePhysical,
                () -> {
                    Connection c;
                    try {
                        c = driver.connect(url, props);
                    } catch (SQLException e) {
                        throw new ApiException(400, "connect failed: " + e.getMessage(),
                                "sql_error", ApiException.sqlExtras(e));
                    }
                    if (c == null) {
                        throw new ApiException(400, "driver " + driver.getClass().getName()
                                + " refused URL '" + url + "' (check user/password/host)");
                    }
                    session.applyState(c);
                    return c;
                },
                session::applyState,
                TimeUnit.SECONDS.toMillis(cfg.keepaliveInterval()),
                session::validateConn);
        session.setKeepaliveTimeout(cfg.keepaliveTimeout());
        session.attachPool(pool);

        // Open one connection up front: validates credentials eagerly and captures
        // the product name used for dialect-specific parameter binding.
        PooledConn warm = pool.borrow(true, false);
        try {
            session.initProductName(warm.conn.getMetaData().getDatabaseProductName());
        } catch (SQLException e) {
            session.initProductName("");
        } finally {
            pool.releaseSafely(warm);
        }
        // eager dialect-ping so the create response carries truthful active info
        PooledConn probe = pool.borrow(true, false);
        try {
            session.validateConn(probe);
        } finally {
            pool.releaseSafely(probe);
        }

        sessions.put(id, session);
        if (!autoCommit) {
            session.beginTxn(null); // legacy semantics: autoCommit=false == start a transaction now
        }
        return session;
    }

    JdbcSession require(String id) {
        JdbcSession s = sessions.get(id);
        if (s == null) {
            throw new ApiException(404, "unknown connection id: " + id + " (closed or never opened)");
        }
        return s;
    }

    boolean close(String id) {
        JdbcSession s = sessions.remove(id);
        if (s == null) return false;
        s.close();
        return true;
    }

    List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (JdbcSession s : sessions.values()) {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("id", s.id);
            m.put("url", HttpApi.redactUrl(s.url));
            m.put("user", s.user);
            m.put("inTransaction", s.inTransaction());
            m.put("dialect", s.resolvedDialect());
            m.put("active", s.isActive());
            m.put("lastUsedAt", java.time.Instant.ofEpochMilli(s.lastUsedAt()).toString());
            m.put("lastValidatedAt", s.lastValidatedAt() > 0
                    ? java.time.Instant.ofEpochMilli(s.lastValidatedAt()).toString() : null);
            m.put("createdAt", java.time.Instant.ofEpochMilli(s.createdAt).toString());
            out.add(m);
        }
        return out;
    }

    int size() {
        return sessions.size();
    }

    int livePhysicalConnections() {
        return livePhysical.get();
    }

    void closeAll() {
        for (String id : sessions.keySet()) {
            close(id);
        }
        reaper.shutdownNow();
    }

    private void reapIdle() {
        long now = System.currentTimeMillis();
        for (JdbcSession s : sessions.values()) {
            if (now - s.lastUsedAt() < TimeUnit.SECONDS.toMillis(cfg.idleTimeoutSeconds())) continue;
            if (s.txnLock().tryLock()) { // never reap mid-transaction or mid-statement
                try {
                    if (now - s.lastUsedAt() >= TimeUnit.SECONDS.toMillis(cfg.idleTimeoutSeconds())) {
                        sessions.remove(s.id);
                        s.close();
                    }
                } finally {
                    s.txnLock().unlock();
                }
            }
        }
    }

    static JsonObject summary(JdbcSession s) {
        JsonObject o = new JsonObject();
        o.addProperty("id", s.id);
        o.addProperty("url", HttpApi.redactUrl(s.url));
        o.addProperty("user", s.user);
        o.addProperty("inTransaction", s.inTransaction());
        o.addProperty("dialect", s.resolvedDialect());
        o.addProperty("active", s.isActive());
        if (s.lastValidatedAt() > 0) {
            o.addProperty("lastValidatedAt", java.time.Instant.ofEpochMilli(s.lastValidatedAt()).toString());
        }
        PhysicalPool p = s.pool();
        if (p != null) {
            o.addProperty("poolIdle", p.idleCount());
            o.addProperty("poolInUse", p.inUseCount());
            o.addProperty("poolPinned", p.pinnedCount());
        }
        return o;
    }

    /** Periodic liveness probe of every session's idle physical connections. */
    private void keepAliveSweep() {
        int dropped = 0;
        for (JdbcSession s : sessions.values()) {
            if (s.inTransaction() || s.isClosed() || s.pool() == null) continue;
            try {
                dropped += s.pool().sweepIdle();
            } catch (RuntimeException e) {
                System.err.println("[warn] keepalive sweep failed for " + s.id + ": " + e);
            }
        }
        if (dropped > 0) {
            System.out.println("[keepalive] dropped " + dropped + " dead physical connection(s)");
        }
    }
}
