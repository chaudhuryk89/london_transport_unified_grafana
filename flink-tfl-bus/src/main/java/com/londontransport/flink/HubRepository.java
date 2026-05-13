package com.londontransport.flink;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** JDBC access to hub_stops / bus_positions (mirrors Python {@code db.py}). */
public final class HubRepository {

    private static final int UPSERT_BATCH = 500;

    private HubRepository() {}

    public static List<String> loadHubNaptanIds(JobParams p) throws Exception {
        try (Connection conn = open(p)) {
            try (var st = conn.prepareStatement("SELECT naptan_id FROM hub_stops ORDER BY naptan_id");
                    ResultSet rs = st.executeQuery()) {
                List<String> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getString(1));
                }
                return ids;
            }
        }
    }

    /** naptan_id -> (lat, lon) same as Python tuple order. */
    public static Map<String, double[]> loadHubLatLonMap(JobParams p) throws Exception {
        try (Connection conn = open(p)) {
            try (var st = conn.prepareStatement("SELECT naptan_id, lat, lon FROM hub_stops");
                    ResultSet rs = st.executeQuery()) {
                Map<String, double[]> out = new LinkedHashMap<>();
                while (rs.next()) {
                    out.put(rs.getString(1), new double[] {rs.getDouble(2), rs.getDouble(3)});
                }
                return out;
            }
        }
    }

    public static void upsertHubStops(JobParams p, List<HubStopRow> stops) throws Exception {
        String sql =
                "INSERT INTO hub_stops (naptan_id, common_name, stop_type, lat, lon, hub_naptan_code, updated_at) "
                        + "VALUES (?,?,?,?,?,?, now()) "
                        + "ON CONFLICT (naptan_id) DO UPDATE SET "
                        + "common_name = EXCLUDED.common_name, stop_type = EXCLUDED.stop_type, "
                        + "lat = EXCLUDED.lat, lon = EXCLUDED.lon, hub_naptan_code = EXCLUDED.hub_naptan_code, "
                        + "updated_at = now()";
        try (Connection conn = open(p)) {
            conn.setAutoCommit(false);
            for (int offset = 0; offset < stops.size(); offset += UPSERT_BATCH) {
                int hi = Math.min(offset + UPSERT_BATCH, stops.size());
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    for (int k = offset; k < hi; k++) {
                        HubStopRow hs = stops.get(k);
                        ps.setString(1, hs.naptanId);
                        ps.setString(2, hs.commonName);
                        ps.setString(3, hs.stopType);
                        ps.setDouble(4, hs.lat);
                        ps.setDouble(5, hs.lon);
                        ps.setString(6, hs.hubNaptanCode);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
            }
        }
    }

    private static Connection open(JobParams p) throws Exception {
        return DriverManager.getConnection(p.jdbcUrlForDriver(), p.jdbcUser, p.jdbcPassword);
    }

    public static final class HubStopRow {
        public final String naptanId;
        public final String commonName;
        public final String stopType;
        public final double lat;
        public final double lon;
        public final String hubNaptanCode;

        public HubStopRow(
                String naptanId,
                String commonName,
                String stopType,
                double lat,
                double lon,
                String hubNaptanCode) {
            this.naptanId = naptanId;
            this.commonName = commonName;
            this.stopType = stopType;
            this.lat = lat;
            this.lon = lon;
            this.hubNaptanCode = hubNaptanCode;
        }
    }

    /**
     * Parse a TfL StopPoint JSON object as a bus-serving stop for the ETL catalog. Accepts either
     * {@code modes} containing {@code bus}, or (when TfL omits modes on StopPoint/Type pages) a
     * {@code stopType} id that clearly denotes a bus/coach stop.
     */
    public static Optional<HubStopRow> parseIfBusHub(JsonNode sp) {
        if (!isBusServingStop(sp)) {
            return Optional.empty();
        }
        String nid = text(sp, "naptanId").orElseGet(() -> text(sp, "id").orElse(null));
        if (nid == null) {
            return Optional.empty();
        }
        JsonNode latN = sp.get("lat");
        JsonNode lonN = sp.get("lon");
        Double lat = readCoord(latN);
        Double lon = readCoord(lonN);
        if (lat == null || lon == null) {
            return Optional.empty();
        }
        return Optional.of(
                new HubStopRow(
                        nid,
                        text(sp, "commonName").orElse(null),
                        text(sp, "stopType").orElse(null),
                        lat,
                        lon,
                        text(sp, "hubNaptanCode").orElse(null)));
    }

    private static boolean isBusServingStop(JsonNode sp) {
        if (modesHasBus(sp)) {
            return true;
        }
        Optional<String> st = text(sp, "stopType");
        if (st.isEmpty()) {
            return false;
        }
        String t = st.get().toLowerCase(Locale.ROOT);
        return t.contains("bus")
                || t.contains("onstreet")
                || t.contains("coach")
                || t.equals("transportinterchange");
    }

    private static boolean modesHasBus(JsonNode sp) {
        JsonNode modes = sp.get("modes");
        if (modes == null || !modes.isArray()) {
            return false;
        }
        for (JsonNode m : modes) {
            if (m.isTextual() && "bus".equalsIgnoreCase(m.asText())) {
                return true;
            }
        }
        return false;
    }

    private static Optional<String> text(JsonNode sp, String key) {
        JsonNode v = sp.get(key);
        if (v == null || v.isNull()) {
            return Optional.empty();
        }
        return Optional.of(v.asText());
    }

    private static Double readCoord(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        if (n.isNumber()) {
            return n.asDouble();
        }
        if (n.isTextual()) {
            try {
                return Double.parseDouble(n.asText().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
