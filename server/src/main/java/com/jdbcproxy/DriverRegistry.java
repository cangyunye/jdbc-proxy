package com.jdbcproxy;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

/**
 * Loads JDBC driver jars from a directory (URLClassLoader + ServiceLoader) and
 * resolves the right {@link Driver} for a JDBC URL.
 *
 * <p>Deliberately avoids {@code DriverManager}: it silently skips drivers whose
 * classes are not loadable by the caller's class loader, which breaks child-first
 * jar loading. We keep our own registry and call {@link Driver#connect} directly.</p>
 */
final class DriverRegistry {
    /** URL prefix -> driver class name, used as a deterministic fast path before acceptsURL() scanning. */
    private static final Map<String, String> KNOWN_PREFIXES = Map.ofEntries(
            Map.entry("jdbc:mysql:", "com.mysql.cj.jdbc.Driver"),
            Map.entry("jdbc:mariadb:", "org.mariadb.jdbc.Driver"),
            Map.entry("jdbc:postgresql:", "org.postgresql.Driver"),
            Map.entry("jdbc:oracle:", "oracle.jdbc.OracleDriver"),
            Map.entry("jdbc:opengauss:", "org.opengauss.Driver"),
            Map.entry("jdbc:gaussdb:", "org.opengauss.Driver"),
            Map.entry("jdbc:oceanbase:", "com.oceanbase.jdbc.Driver"),
            Map.entry("jdbc:h2:", "org.h2.Driver"),
            Map.entry("jdbc:sqlite:", "org.sqlite.JDBC"),
            Map.entry("jdbc:sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
            Map.entry("jdbc:dm:", "dm.jdbc.driver.DmDriver"),
            Map.entry("jdbc:kingbase8:", "com.kingbase8.Driver"),
            Map.entry("jdbc:db2:", "com.ibm.db2.jcc.DB2Driver"),
            Map.entry("jdbc:derby:", "org.apache.derby.jdbc.AutoloadedDriver"));

    private final List<Driver> drivers = new CopyOnWriteArrayList<>();
    private final URLClassLoader classLoader;
    private final Map<String, Driver> byClassName = Collections.synchronizedMap(new LinkedHashMap<>());

    private DriverRegistry(URLClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    static DriverRegistry load(Path dir, List<String> warnings) throws IOException {
        List<URL> urls = new ArrayList<>();
        if (dir != null && Files.isDirectory(dir)) {
            try (Stream<Path> files = Files.list(dir)) {
                files.filter(p -> {
                    String n = p.getFileName().toString().toLowerCase();
                    return n.endsWith(".jar") || n.endsWith(".zip");
                }).sorted().forEach(p -> {
                    try {
                        urls.add(p.toUri().toURL());
                    } catch (Exception e) {
                        warnings.add("skipped jar " + p + ": " + e.getMessage());
                    }
                });
            }
        } else if (dir != null) {
            warnings.add("drivers dir not found: " + dir.toAbsolutePath());
        }

        ClassLoader parent = DriverRegistry.class.getClassLoader();
        URLClassLoader loader = urls.isEmpty() ? null : new URLClassLoader(urls.toArray(new URL[0]), parent);
        DriverRegistry registry = new DriverRegistry(loader);

        // JDBC 4 service discovery inside the jar dir
        if (loader != null) {
            for (Driver d : ServiceLoader.load(Driver.class, loader)) {
                registry.register(d);
            }
        }
        // Well-known class names, in case a jar lacks META-INF/services/java.sql.Driver
        for (String cn : KNOWN_PREFIXES.values().stream().distinct().toList()) {
            registry.ensureLoaded(cn);
        }
        return registry;
    }

    private void register(Driver d) {
        if (d == null) return;
        drivers.add(d);
        byClassName.putIfAbsent(d.getClass().getName(), d);
    }

    /** Loads and registers a driver by class name if present; returns existing instance otherwise. */
    Driver ensureLoaded(String className) {
        if (className == null || className.isBlank()) return null;
        Driver existing = byClassName.get(className);
        if (existing != null) return existing;
        ClassLoader cl = classLoader != null ? classLoader : DriverRegistry.class.getClassLoader();
        try {
            Class<?> c = Class.forName(className, true, cl);
            Object inst = c.getDeclaredConstructor().newInstance();
            if (inst instanceof Driver d) {
                register(d);
                return d;
            }
        } catch (Throwable ignored) {
            // class not present in the jars dir / classpath
        }
        return null;
    }

    /** Finds a driver for the URL. Explicit class name wins, then prefix map, then acceptsURL scan. */
    Driver find(String url, String driverClass) {
        if (driverClass != null && !driverClass.isBlank()) {
            Driver d = ensureLoaded(driverClass);
            if (d == null) {
                throw new ApiException(400, "driver class not found or not a java.sql.Driver: " + driverClass
                        + " (available: " + classNames() + ")");
            }
            return d;
        }
        String lower = url.toLowerCase();
        for (Map.Entry<String, String> e : KNOWN_PREFIXES.entrySet()) {
            if (lower.startsWith(e.getKey())) {
                Driver d = ensureLoaded(e.getValue());
                if (d != null) return d;
            }
        }
        for (Driver d : drivers) {
            try {
                if (d.acceptsURL(url)) return d;
            } catch (Throwable ignored) {
                // a misbehaving driver must not break resolution for others
            }
        }
        throw new ApiException(400, "no JDBC driver accepts URL '" + url + "'. loaded drivers: [" + classNames() + "]");
    }

    List<Map<String, Object>> describe() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Driver d : drivers) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("className", d.getClass().getName());
            m.put("majorVersion", d.getMajorVersion());
            m.put("minorVersion", d.getMinorVersion());
            m.put("jdbcCompliant", d.jdbcCompliant());
            out.add(m);
        }
        return out;
    }

    String classNames() {
        return String.join(", ", drivers.stream().map(d -> d.getClass().getName()).toList());
    }

    int jarCount() {
        return classLoader == null ? 0 : classLoader.getURLs().length;
    }
}
