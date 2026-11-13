package com.jdbcproxy;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

/** Entry point: parse config, load drivers, start the HTTP server. */
public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        try {
            Config cfg = Config.fromArgs(args, System.getenv());
            List<String> warnings = new ArrayList<>();
            DriverRegistry registry = DriverRegistry.load(cfg.driversDir(), warnings);
            DriverManager.setLoginTimeout(cfg.connectTimeoutSeconds());

            ConnectionManager manager = new ConnectionManager(cfg, registry);
            HttpApi api = new HttpApi(cfg, manager, registry);
            api.start();

            System.out.println("jdbc-proxy-server " + HttpApi.VERSION + " listening on http://" + cfg.host() + ":" + cfg.port());
            System.out.println("drivers dir: " + cfg.driversDir().toAbsolutePath() + " (" + registry.jarCount() + " jars, "
                    + registry.describe().size() + " drivers)");
            System.out.println("loaded drivers: " + registry.classNames());
            System.out.println("pool: size=" + cfg.poolSize() + " per session, reserve-reads=" + cfg.reserveReads()
                    + ", max-physical=" + cfg.maxPhysical() + ", http threads="
                    + (cfg.threads() <= 0 ? "virtual" : cfg.threads()));
            for (String w : warnings) {
                System.out.println("[warn] " + w);
            }
            System.out.println("press Ctrl+C to stop");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nshutting down: closing " + manager.size() + " connection(s)");
                api.stop();
                manager.closeAll();
            }, "jdbc-proxy-shutdown"));
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println();
            System.err.println("options:");
            System.err.println("  --host <addr>            bind address (default 127.0.0.1)");
            System.err.println("  --port <n>               HTTP port (default 8089)");
            System.err.println("  --drivers-dir <dir>      directory with JDBC driver jars (default jars)");
            System.err.println("  --api-key <key>          require X-Api-Key header (optional)");
            System.err.println("  --max-connections <n>    max open connections (default 64)");
            System.err.println("  --idle-timeout <secs>    auto-close idle connections (default 1800)");
            System.err.println("  --max-rows <n>           hard cap on rows per response (default 10000)");
            System.err.println("  --threads <n>            HTTP worker threads");
            System.err.println("  --connect-timeout <secs> driver login timeout (default 10)");
            System.exit(2);
        } catch (Exception e) {
            System.err.println("fatal: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
