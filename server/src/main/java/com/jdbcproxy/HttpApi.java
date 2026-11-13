package com.jdbcproxy;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

/** HTTP layer: routing, auth, JSON plumbing. All routes are JSON over HTTP. */
final class HttpApi {
    static final String VERSION = "1.1.0";
    private static final int MAX_BODY_BYTES = 64 * 1024 * 1024;

    private final Config cfg;
    private final ConnectionManager manager;
    private final DriverRegistry registry;
    private final MetadataService metadata;
    private HttpServer server;

    HttpApi(Config cfg, ConnectionManager manager, DriverRegistry registry) {
        this.cfg = cfg;
        this.manager = manager;
        this.registry = registry;
        this.metadata = new MetadataService(cfg);
    }

    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(cfg.host(), cfg.port()), 128);
        server.createContext("/", this::handle);
        // 0 = virtual threads: cheap concurrency for many parallel client threads;
        // N > 0 = fixed platform-thread pool
        server.setExecutor(cfg.threads() <= 0
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newFixedThreadPool(cfg.threads()));
        server.start();
    }

    void stop() {
        if (server != null) server.stop(0);
    }

    // ---------------------------------------------------------------- dispatch

    private void handle(HttpExchange ex) {
        try {
            String path = ex.getRequestURI().getPath();
            if (!path.equals("/health")) { // liveness probes stay unauthenticated
                checkAuth(ex);
            }
            String method = ex.getRequestMethod();
            String[] seg = path.split("/"); // ["", "api", "connections", "{id}", "..."]

            if (path.equals("/health")) {
                requireMethod(method, "GET");
                send(ex, 200, health());
            } else if (path.equals("/drivers")) {
                requireMethod(method, "GET");
                send(ex, 200, drivers());
            } else if (path.equals("/api/connections")) {
                switch (method) {
                    case "POST" -> send(ex, 201, createConnection(readBody(ex)));
                    case "GET" -> send(ex, 200, listConnections());
                    default -> throw new ApiException(405, "method " + method + " not allowed on /api/connections");
                }
            } else if (seg.length == 4 && seg[1].equals("api") && seg[2].equals("connections")) {
                String id = seg[3];
                switch (method) {
                    case "GET" -> send(ex, 200, connectionInfo(id));
                    case "DELETE" -> send(ex, 200, closeConnection(id));
                    default -> throw new ApiException(405, "method " + method + " not allowed here");
                }
            } else if (seg.length == 5 && seg[1].equals("api") && seg[2].equals("connections") && method.equals("POST")) {
                String id = seg[3];
                switch (seg[4]) {
                    case "execute" -> send(ex, 200, StatementExecutor.execute(manager.require(id), readBody(ex), cfg));
                    case "batch" -> send(ex, 200, StatementExecutor.batch(manager.require(id), readBody(ex), cfg));
                    case "transaction" -> send(ex, 200, StatementExecutor.transaction(manager.require(id), readBody(ex)));
                    default -> throw new ApiException(404, "unknown endpoint: " + path);
                }
            } else if (seg.length == 6 && seg[1].equals("api") && seg[2].equals("connections")
                    && seg[4].equals("metadata") && method.equals("GET")) {
                send(ex, 200, metadataDispatch(manager.require(seg[3]), seg[5], queryParams(ex)));
            } else if (path.equals("/") && method.equals("GET")) {
                send(ex, 200, index());
            } else {
                throw new ApiException(404, "no route: " + method + " " + path);
            }
        } catch (ApiException e) {
            send(ex, e.status, e.toJson());
        } catch (JsonParseException e) {
            send(ex, 400, new ApiException(400, "invalid JSON body: " + e.getMessage()).toJson());
        } catch (SQLException e) {
            send(ex, 400, new ApiException(400, e.getMessage(), "sql_error", ApiException.sqlExtras(e)).toJson());
        } catch (Exception e) {
            System.err.println("[error] " + ex.getRequestMethod() + " " + ex.getRequestURI().getPath() + ": " + e);
            e.printStackTrace();
            send(ex, 500, new ApiException(500, "internal error: " + e, "internal_error", Map.of()).toJson());
        } finally {
            ex.close();
        }
    }

    private void requireMethod(String method, String allowed) {
        if (!method.equals(allowed)) {
            throw new ApiException(405, "method " + method + " not allowed (expected " + allowed + ")");
        }
    }

    private JsonObject metadataDispatch(JdbcSession s, String name, Map<String, String> q)
            throws SQLException {
        switch (name) {
            case "dialect": return dialectOf(s);
            case "tables": return metadata.tables(s, q);
            case "columns": return metadata.columns(s, q);
            case "primaryKeys": return metadata.primaryKeys(s, q);
            case "foreignKeys": return metadata.foreignKeys(s, q);
            case "indexes": return metadata.indexes(s, q);
            case "schemas": return metadata.schemas(s, q);
            case "catalogs": return metadata.catalogs(s, q);
            case "tableTypes": return metadata.tableTypes(s, q);
            case "typeInfo": return metadata.typeInfo(s, q);
            default: throw new ApiException(404, "unknown metadata query '" + name + "' (supported: dialect, tables, columns, "
                    + "primaryKeys, foreignKeys, indexes, schemas, catalogs, tableTypes, typeInfo)");
        }
    }

    private JsonObject dialectOf(JdbcSession s) throws SQLException {
        s.touch();
        return metadata.dialect(s);
    }

    // ---------------------------------------------------------------- handlers

    private JsonObject createConnection(JsonObject req) {
        String url = str(req, "url", null);
        if (url == null || url.isBlank()) throw new ApiException(400, "'url' (JDBC URL) is required");
        String user = str(req, "user", null);
        String password = str(req, "password", null);
        String driverClass = str(req, "driverClass", null);
        Properties props = new Properties();
        JsonElement pe = req.get("properties");
        if (pe != null && pe.isJsonObject()) {
            for (Map.Entry<String, com.google.gson.JsonElement> e : pe.getAsJsonObject().entrySet()) {
                if (e.getValue().isJsonPrimitive()) {
                    props.setProperty(e.getKey(), e.getValue().getAsString());
                }
            }
        }

        boolean autoCommit = req.get("autoCommit") == null || req.get("autoCommit").getAsBoolean();
        boolean readOnly = req.get("readOnly") != null && req.get("readOnly").getAsBoolean();
        Integer isolation = StatementExecutor.optInt(req, "isolation");
        if (isolation == null) {
            String isoName = str(req, "isolation", null);
            if (isoName != null) isolation = isoLevelByName(isoName);
        }

        JdbcSession s = manager.create(url, user, password, driverClass, props,
                autoCommit, readOnly, isolation, str(req, "dialect", null));
        JsonObject out = new JsonObject();
        out.addProperty("connectionId", s.id);
        out.addProperty("url", redactUrl(s.url));
        out.addProperty("user", s.user);
        out.addProperty("dialect", s.resolvedDialect());
        out.addProperty("active", s.isActive());
        if (s.lastValidatedAt() > 0) {
            out.addProperty("lastValidatedAt",
                    java.time.Instant.ofEpochMilli(s.lastValidatedAt()).toString());
        }
        return out;
    }

    private int isoLevelByName(String name) {
        return switch (name.trim().toUpperCase()) {
            case "NONE" -> java.sql.Connection.TRANSACTION_NONE;
            case "READ_UNCOMMITTED" -> java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
            case "READ_COMMITTED" -> java.sql.Connection.TRANSACTION_READ_COMMITTED;
            case "REPEATABLE_READ" -> java.sql.Connection.TRANSACTION_REPEATABLE_READ;
            case "SERIALIZABLE" -> java.sql.Connection.TRANSACTION_SERIALIZABLE;
            default -> throw new ApiException(400, "unknown isolation '" + name
                    + "': NONE|READ_UNCOMMITTED|READ_COMMITTED|REPEATABLE_READ|SERIALIZABLE");
        };
    }

    private JsonObject listConnections() {
        JsonObject out = new JsonObject();
        JsonArray arr = new JsonArray();
        for (Map<String, Object> m : manager.list()) {
            JsonObject o = new JsonObject();
            m.forEach((k, v) -> {
                if (v instanceof Integer i) o.addProperty(k, i);
                else o.addProperty(k, String.valueOf(v));
            });
            arr.add(o);
        }
        out.add("connections", arr);
        out.addProperty("count", arr.size());
        return out;
    }

    private JsonObject connectionInfo(String id) {
        return ConnectionManager.summary(manager.require(id));
    }

    private JsonObject closeConnection(String id) {
        JsonObject o = new JsonObject();
        boolean closed = manager.close(id);
        o.addProperty("closed", closed);
        if (!closed) o.addProperty("note", "connection " + id + " was already closed");
        return o;
    }

    private JsonObject health() {
        JsonObject o = new JsonObject();
        o.addProperty("status", "ok");
        o.addProperty("service", "jdbc-proxy");
        o.addProperty("version", VERSION);
        o.addProperty("openConnections", manager.size());
        o.addProperty("physicalConnections", manager.livePhysicalConnections());
        o.addProperty("maxConnections", cfg.maxConnections());
        o.addProperty("maxPhysical", cfg.maxPhysical());
        o.addProperty("maxRows", cfg.maxRows());
        o.addProperty("poolSize", cfg.poolSize());
        o.addProperty("keepaliveInterval", cfg.keepaliveInterval());
        o.addProperty("idleTimeout", cfg.idleTimeoutSeconds());
        return o;
    }

    private JsonObject drivers() {
        JsonObject o = new JsonObject();
        JsonArray arr = new JsonArray();
        for (Map<String, Object> m : registry.describe()) {
            JsonObject d = new JsonObject();
            m.forEach((k, v) -> {
                if (v instanceof Integer i) d.addProperty(k, i);
                else if (v instanceof Boolean b) d.addProperty(k, b);
                else d.addProperty(k, String.valueOf(v));
            });
            arr.add(d);
        }
        o.add("drivers", arr);
        return o;
    }

    private JsonObject index() {
        JsonObject o = new JsonObject();
        o.addProperty("service", "jdbc-proxy");
        o.addProperty("version", VERSION);
        JsonArray eps = new JsonArray();
        for (String e : List.of(
                "GET  /health",
                "GET  /drivers",
                "POST /api/connections            {url, user, password, driverClass?, properties?, autoCommit?, readOnly?, isolation?}",
                "GET  /api/connections            list open connections",
                "GET  /api/connections/{id}       connection info",
                "DELETE /api/connections/{id}     close connection",
                "POST /api/connections/{id}/execute    {sql, params?, intent?=\"read\"|\"write\", maxRows?, fetchSize?, timeoutSeconds?}",
                "POST /api/connections/{id}/batch      {sql, batch:[[params],...]}",
                "POST /api/connections/{id}/transaction {action: begin|commit|rollback|setAutoCommit|setIsolation}",
                "GET  /api/connections/{id}/metadata/{dialect|tables|columns|primaryKeys|foreignKeys|indexes|schemas|catalogs|tableTypes|typeInfo}?max=")) {
            eps.add(new JsonPrimitive(e));
        }
        o.add("endpoints", eps);
        return o;
    }

    // ---------------------------------------------------------------- plumbing

    private void checkAuth(HttpExchange ex) {
        if (cfg.apiKey() == null) return;
        String given = ex.getRequestHeaders().getFirst("X-Api-Key");
        if (given == null) {
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            if (auth != null && auth.regionMatches(true, 0, "Bearer ", 0, 7)) {
                given = auth.substring(7).trim();
            }
        }
        if (given == null || !MessageDigest.isEqual(
                cfg.apiKey().getBytes(StandardCharsets.UTF_8), given.getBytes(StandardCharsets.UTF_8))) {
            throw new ApiException(401, "missing or invalid X-Api-Key (or Authorization: Bearer) header");
        }
    }

    /** Strips credentials embedded in JDBC URLs before echoing them to clients. */
    static String redactUrl(String url) {
        if (url == null) return null;
        String out = url.replaceAll("(?i)://([^/@:]+):([^/@]+)@", "://***:***@");
        out = out.replaceAll("(?i)(password=)[^&;]+", "$1***");
        return out;
    }

    private JsonObject readBody(HttpExchange ex) throws IOException {
        InputStream in = ex.getRequestBody();
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int n;
        while ((n = in.read(chunk)) > 0) {
            buf.write(chunk, 0, n);
            if (buf.size() > MAX_BODY_BYTES) {
                throw new ApiException(413, "request body exceeds " + MAX_BODY_BYTES + " bytes");
            }
        }
        String body = buf.toString(StandardCharsets.UTF_8);
        if (body.isBlank()) return new JsonObject();
        return JsonParser.parseString(body).getAsJsonObject();
    }

    private Map<String, String> queryParams(HttpExchange ex) {
        Map<String, String> q = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw == null || raw.isBlank()) return q;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                q.put(URLDecoder.decode(pair, StandardCharsets.UTF_8), "");
            } else {
                q.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return q;
    }

    private static String str(JsonObject o, String key, String def) {
        com.google.gson.JsonElement el = o.get(key);
        return el != null && el.isJsonPrimitive() ? el.getAsString() : def;
    }

    private void send(HttpExchange ex, int status, JsonObject payload) {
        try {
            byte[] body = payload.toString().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            if (body.length == 0) {
                ex.sendResponseHeaders(status, -1);
            } else {
                ex.sendResponseHeaders(status, body.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(body);
                }
            }
        } catch (IOException e) {
            System.err.println("[warn] failed to write response: " + e.getMessage());
        }
    }
}
