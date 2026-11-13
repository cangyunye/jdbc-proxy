package com.jdbcproxy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;

/**
 * One physical JDBC connection owned by a {@link PhysicalPool}.
 * Exclusively held by a single worker thread between borrow/release,
 * with a per-connection LRU PreparedStatement cache.
 */
final class PooledConn {
    static final int PS_CACHE_MAX = 32;

    final Connection conn;
    final LinkedHashMap<String, PreparedStatement> psCache = new LinkedHashMap<>(16, 0.75f, true);
    private boolean discarded;

    /** When this connection was returned to the idle list (creation time until first release). */
    volatile long idleSince = System.currentTimeMillis();
    /** Last successful dialect-ping / usage, for the connection's active info. */
    volatile long lastValidatedAt = 0L;

    PooledConn(Connection conn) {
        this.conn = conn;
    }

    /** Cached PreparedStatement (created with RETURN_GENERATED_KEYS for uniformity). */
    PreparedStatement prepared(String sql) throws SQLException {
        PreparedStatement ps = psCache.get(sql);
        if (ps == null) {
            ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            psCache.put(sql, ps);
            while (psCache.size() > PS_CACHE_MAX) {
                String eldestKey = psCache.keySet().iterator().next();
                closeQuietly(psCache.remove(eldestKey));
            }
        }
        return ps;
    }

    boolean isDiscarded() {
        return discarded;
    }

    void markDiscarded() {
        this.discarded = true;
    }

    private static void closeQuietly(PreparedStatement ps) {
        try {
            ps.close();
        } catch (SQLException ignored) {
        }
    }
}
