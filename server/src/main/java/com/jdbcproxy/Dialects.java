package com.jdbcproxy;

/**
 * Database dialect handling for compat-mode databases.
 *
 * <p>OceanBase runs in MySQL or Oracle mode; OpenGauss speaks a PostgreSQL
 * dialect. The JDBC URL prefix alone cannot tell them apart, so clients may
 * declare the dialect explicitly when opening a connection
 * ({@code POST /api/connections {"dialect": "oceanbase_mysql", ...}}).
 * When omitted, the dialect is inferred from the driver-reported product name
 * and URL prefix.</p>
 *
 * <p>Everything downstream (schema/catalog defaults, parameter binding quirks,
 * ping statements) keys off the resolved <b>family</b>: mysql | postgresql | oracle.</p>
 */
final class Dialects {
    /** Explicitly supported declaration values (client-facing). */
    static final java.util.Set<String> KNOWN_HINTS = java.util.Set.of(
            "auto",
            // mysql family
            "mysql", "mariadb", "oceanbase_mysql",
            // postgresql family
            "postgresql", "pg", "opengauss", "gaussdb", "oceanbase_pg",
            // oracle family
            "oracle", "oceanbase_oracle");

    private Dialects() {
    }

    /** Validates and keeps the client's hint; null/blank/"auto" means infer. */
    static String normalizeHint(String hint) {
        if (hint == null || hint.isBlank() || "auto".equalsIgnoreCase(hint)) return null;
        String h = hint.trim().toLowerCase();
        if (!KNOWN_HINTS.contains(h)) {
            throw new ApiException(400, "unknown dialect '" + hint + "'; supported: " + KNOWN_HINTS
                    + " (or omit for auto-detection)");
        }
        return h;
    }

    /** Coarse family used for binding/defaults decisions. */
    static String familyOf(String hintOrInferred) {
        if (hintOrInferred == null) return "unknown";
        switch (hintOrInferred) {
            case "mysql": case "mariadb": case "oceanbase_mysql":
                return "mysql";
            case "postgresql": case "pg": case "opengauss": case "gaussdb": case "oceanbase_pg":
                return "postgresql";
            case "oracle": case "oceanbase_oracle":
                return "oracle";
            default:
                return "unknown";
        }
    }

    /** Inference fallback: driver product name first, then URL prefix. Inputs are normalized internally. */
    static String infer(String product, String url) {
        String p = product == null ? "" : product.toLowerCase();
        if (!p.isEmpty()) {
            if (p.contains("oracle")) return "oracle";
            if (p.contains("opengauss") || p.contains("gauss")) return "opengauss";
            if (p.contains("postgres")) return "postgresql";
            if (p.contains("mysql")) return "mysql";
            if (p.contains("mariadb")) return "mariadb";
        }
        String u = url == null ? "" : url.toLowerCase();
        if (u.startsWith("jdbc:oracle")) return "oracle";
        if (u.startsWith("jdbc:opengauss") || u.startsWith("jdbc:gaussdb")) return "opengauss";
        if (u.startsWith("jdbc:postgresql")) return "postgresql";
        if (u.startsWith("jdbc:mysql")) return "mysql";
        if (u.startsWith("jdbc:mariadb")) return "mariadb";
        return "unknown";
    }
}
