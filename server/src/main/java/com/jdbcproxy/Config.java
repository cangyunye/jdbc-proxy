package com.jdbcproxy;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Server configuration, sourced from CLI args with environment variable fallbacks. */
record Config(String host, int port, Path driversDir, String apiKey,
              int maxConnections, int idleTimeoutSeconds, int maxRows,
              int threads, int connectTimeoutSeconds,
              int poolSize, int reserveReads, long borrowWaitMs,
              int maxPhysical, int maxBatch,
              int keepaliveInterval, int keepaliveTimeout) {

    static Config fromArgs(String[] args, Map<String, String> env) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument: " + a);
            }
            String body = a.substring(2);
            int eq = body.indexOf('=');
            if (eq >= 0) {
                opts.put(body.substring(0, eq), body.substring(eq + 1));
            } else {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for --" + body);
                }
                opts.put(body, args[++i]);
            }
        }

        String host = pick(opts, env, "host", "JDBC_PROXY_HOST", "127.0.0.1");
        int port = intOf(pick(opts, env, "port", "JDBC_PROXY_PORT", "8089"), "port");
        String driversDir = pick(opts, env, "drivers-dir", "JDBC_PROXY_DRIVERS_DIR", "jars");
        String apiKey = pick(opts, env, "api-key", "JDBC_PROXY_API_KEY", null);
        int maxConnections = intOf(pick(opts, env, "max-connections", "JDBC_PROXY_MAX_CONNECTIONS", "64"), "max-connections");
        int idleTimeout = intOf(pick(opts, env, "idle-timeout", "JDBC_PROXY_IDLE_TIMEOUT", "1800"), "idle-timeout");
        int maxRows = intOf(pick(opts, env, "max-rows", "JDBC_PROXY_MAX_ROWS", "10000"), "max-rows");
        // 0 = virtual threads (unbounded, IO-friendly); N > 0 = fixed platform pool
        int threads = intOf(pick(opts, env, "threads", "JDBC_PROXY_THREADS", "0"), "threads");
        int connectTimeout = intOf(pick(opts, env, "connect-timeout", "JDBC_PROXY_CONNECT_TIMEOUT", "10"), "connect-timeout");
        int poolSize = intOf(pick(opts, env, "pool-size", "JDBC_PROXY_POOL_SIZE", "8"), "pool-size");
        int reserveReads = intOf(pick(opts, env, "reserve-reads", "JDBC_PROXY_RESERVE_READS", "2"), "reserve-reads");
        long borrowWaitMs = longOf(pick(opts, env, "borrow-wait", "JDBC_PROXY_BORROW_WAIT", "30000"), "borrow-wait");
        int maxPhysical = intOf(pick(opts, env, "max-physical", "JDBC_PROXY_MAX_PHYSICAL", "256"), "max-physical");
        int maxBatch = intOf(pick(opts, env, "max-batch", "JDBC_PROXY_MAX_BATCH", "10000"), "max-batch");
        int keepaliveInterval = intOf(pick(opts, env, "keepalive-interval", "JDBC_PROXY_KEEPALIVE_INTERVAL", "300"), "keepalive-interval");
        int keepaliveTimeout = intOf(pick(opts, env, "keepalive-timeout", "JDBC_PROXY_KEEPALIVE_TIMEOUT", "5"), "keepalive-timeout");

        return new Config(host, port, Path.of(driversDir), (apiKey == null || apiKey.isBlank()) ? null : apiKey,
                maxConnections, idleTimeout, maxRows, threads, connectTimeout,
                poolSize, reserveReads, borrowWaitMs, maxPhysical, maxBatch,
                keepaliveInterval, keepaliveTimeout);
    }

    private static String pick(Map<String, String> opts, Map<String, String> env, String key, String envKey, String def) {
        String v = opts.get(key);
        if (v == null) v = env.get(envKey);
        if (v == null || v.isBlank()) return def;
        return v.trim(); // e.g. "JDBC_PROXY_API_KEY= k " in a dotenv file
    }

    private static int intOf(String v, String name) {
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid integer for --" + name + ": " + v);
        }
    }

    private static long longOf(String v, String name) {
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid integer for --" + name + ": " + v);
        }
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT,
                "jdbc-proxy-server options:%n"
                + "  --host <addr>            bind address (default 127.0.0.1)%n"
                + "  --port <n>               HTTP port (default 8089)%n"
                + "  --drivers-dir <dir>      directory with JDBC driver jars (default jars)%n"
                + "  --api-key <key>          require X-Api-Key or Authorization: Bearer (optional)%n"
                + "  --max-connections <n>    max logical sessions (default 64)%n"
                + "  --idle-timeout <secs>    auto-close idle sessions (default 1800, 0 = never)%n"
                + "  --max-rows <n>           hard cap on rows per response (default 10000)%n"
                + "  --threads <n>            0 = virtual threads (default), N = fixed platform pool%n"
                + "  --connect-timeout <secs> driver login timeout (default 10)%n"
                + "  --pool-size <n>          physical connections per session (default 8)%n"
                + "  --reserve-reads <n>      connections never pinned by transactions (default 2)%n"
                + "  --borrow-wait <ms>       max wait for a pool connection (default 30000)%n"
                + "  --max-physical <n>       global cap on physical connections (default 256)%n"
                + "  --max-batch <n>          max rows per batch request (default 10000)%n"
                + "  --keepalive-interval <s> probe idle connections older than this (default 300, 0 = off)%n"
                + "  --keepalive-timeout <s>  per-probe statement timeout (default 5)%n");
    }
}
