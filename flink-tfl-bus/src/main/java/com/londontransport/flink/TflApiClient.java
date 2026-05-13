package com.londontransport.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** TfL Unified API (mirrors Python {@code TflClient}). */
public final class TflApiClient {

    private static final String TFL_BASE = "https://api.tfl.gov.uk";

    private final JobParams params;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(45)).build();
    private final ObjectMapper mapper = new ObjectMapper();

    public TflApiClient(JobParams params) {
        if (params.tflAppKey.isEmpty()) {
            throw new IllegalArgumentException("TFL_APP_KEY is required");
        }
        this.params = params;
    }

    private String buildQuery(Map<String, String> extra) {
        Map<String, String> q = new LinkedHashMap<>();
        q.put("app_key", params.tflAppKey);
        if (params.tflAppId != null && !params.tflAppId.isEmpty()) {
            q.put("app_id", params.tflAppId);
        }
        if (extra != null) {
            q.putAll(extra);
        }
        return q.entrySet().stream()
                .map(e -> urlEnc(e.getKey()) + "=" + urlEnc(e.getValue()))
                .collect(Collectors.joining("&"));
    }

    private static String urlEnc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private String url(String path, Map<String, String> extraParams) {
        String q = buildQuery(extraParams);
        String sep = path.contains("?") ? "&" : "?";
        return TFL_BASE + path + sep + q;
    }

    public JsonNode getJson(String path, Map<String, String> extraParams) throws IOException, InterruptedException {
        for (int attempt = 0; attempt < 5; attempt++) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url(path, extraParams)))
                    .timeout(Duration.ofSeconds(45))
                    .header("Accept", "application/json")
                    .header("User-Agent", "london-transport-unified-grafana/1.0 (Java Flink hub sync)")
                    .GET()
                    .build();
            HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
            int code = r.statusCode();
            if (code == 429) {
                throw new RuntimeException("TfL rate limited (429): " + path);
            }
            if (code >= 500 && code <= 504) {
                Thread.sleep((long) Math.min(8000, 500 * Math.pow(2, attempt)));
                continue;
            }
            if (code >= 400) {
                throw new IOException("TfL HTTP " + code + " for " + path + " body=" + truncate(r.body(), 200));
            }
            return mapper.readTree(r.body());
        }
        throw new IOException("TfL request failed after retries: " + path);
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }

    public List<String> fetchMetaStopTypes() throws IOException, InterruptedException {
        JsonNode data = getJson("/StopPoint/Meta/StopTypes", null);
        List<String> out = new ArrayList<>();
        if (data != null && data.isArray()) {
            for (JsonNode x : data) {
                out.add(x.asText());
            }
        }
        return out;
    }

    /**
     * One stop type per request. Returns [] on persistent TfL errors (no exception), matching Python.
     */
    public List<JsonNode> fetchStopPointsForType(String stopType, int page) throws IOException, InterruptedException {
        String seg = URLEncoder.encode(stopType, StandardCharsets.UTF_8).replace("+", "%20");
        String path = "/StopPoint/Type/" + seg + "/page/" + page;
        for (int attempt = 0; attempt < 5; attempt++) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url(path, null)))
                    .timeout(Duration.ofSeconds(45))
                    .header("Accept", "application/json")
                    .header("User-Agent", "london-transport-unified-grafana/1.0 (Java Flink)")
                    .GET()
                    .build();
            HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
            int code = r.statusCode();
            if (code == 404) {
                return List.of();
            }
            if (code == 429) {
                throw new RuntimeException("TfL rate limited (429): " + path);
            }
            if (code >= 500 && code <= 504) {
                Thread.sleep((long) Math.min(8000, 500 * Math.pow(2, attempt)));
                continue;
            }
            if (code >= 200 && code < 300) {
                try {
                    JsonNode data = mapper.readTree(r.body());
                    List<JsonNode> rows = extractStopPointRows(data);
                    return rows;
                } catch (IOException e) {
                    return List.of();
                }
            }
            System.out.println(
                    "TfL StopPoint/Type non-OK for '" + stopType + "' page=" + page + ": HTTP " + code + " "
                            + truncate(r.body(), 180));
            return List.of();
        }
        return List.of();
    }

    private static List<JsonNode> extractStopPointRows(JsonNode data) {
        List<JsonNode> rows = new ArrayList<>();
        collectStopPointRows(data, rows);
        return rows;
    }

    private static void collectStopPointRows(JsonNode node, List<JsonNode> rows) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isArray()) {
            for (JsonNode child : node) {
                collectStopPointRows(child, rows);
            }
            return;
        }
        if (!node.isObject()) {
            return;
        }
        if (looksLikeStopPoint(node)) {
            rows.add(node);
            return;
        }
        for (String key : List.of("stopPoints", "stopPoint", "matches", "places")) {
            JsonNode child = node.get(key);
            if (child != null) {
                collectStopPointRows(child, rows);
            }
        }
    }

    private static boolean looksLikeStopPoint(JsonNode node) {
        if (!node.isObject()) {
            return false;
        }
        if (!(node.hasNonNull("naptanId") || node.hasNonNull("id"))) {
            return false;
        }
        JsonNode lat = node.get("lat");
        JsonNode lon = node.get("lon");
        return lat != null && !lat.isNull() && lon != null && !lon.isNull();
    }

    /**
     * Fetch a single page of stop points for a TfL mode (e.g. {@code "bus"}). This is the
     * canonical endpoint for the full London bus stop catalog (~19k+ rows across pages).
     * Returns {@code [size, rows]} where {@code size} is total stop point count when reported.
     */
    public StopPointPage fetchStopPointsForMode(String modes, int page) throws IOException, InterruptedException {
        String seg = URLEncoder.encode(modes, StandardCharsets.UTF_8).replace("+", "%20");
        Map<String, String> extra = new LinkedHashMap<>();
        extra.put("page", Integer.toString(page));
        for (int attempt = 0; attempt < 5; attempt++) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url("/StopPoint/Mode/" + seg, extra)))
                    .timeout(Duration.ofSeconds(60))
                    .header("Accept", "application/json")
                    .header("User-Agent", "london-transport-unified-grafana/1.0 (Java Flink)")
                    .GET()
                    .build();
            HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
            int code = r.statusCode();
            if (code == 404) {
                return new StopPointPage(0, List.of());
            }
            if (code == 429) {
                throw new RuntimeException("TfL rate limited (429): /StopPoint/Mode/" + modes);
            }
            if (code >= 500 && code <= 504) {
                Thread.sleep((long) Math.min(8000, 500 * Math.pow(2, attempt)));
                continue;
            }
            if (code >= 200 && code < 300) {
                JsonNode data = mapper.readTree(r.body());
                int total = data.has("total") && data.get("total").isInt() ? data.get("total").asInt() : 0;
                return new StopPointPage(total, extractStopPointRows(data));
            }
            System.out.println(
                    "TfL StopPoint/Mode non-OK for '" + modes + "' page=" + page + ": HTTP " + code + " "
                            + truncate(r.body(), 180));
            return new StopPointPage(0, List.of());
        }
        return new StopPointPage(0, List.of());
    }

    public static final class StopPointPage {
        public final int total;
        public final List<JsonNode> rows;

        public StopPointPage(int total, List<JsonNode> rows) {
            this.total = total;
            this.rows = rows;
        }
    }

    public JsonNode searchStopPoints(String query, String modes) throws IOException, InterruptedException {
        String seg = URLEncoder.encode(query, StandardCharsets.UTF_8).replace("+", "%20");
        Map<String, String> extra = new LinkedHashMap<>();
        extra.put("modes", modes);
        return getJson("/StopPoint/Search/" + seg, extra);
    }

    public Optional<JsonNode> fetchStopPoint(String stopId) throws IOException, InterruptedException {
        String seg = URLEncoder.encode(stopId, StandardCharsets.UTF_8).replace("+", "%20");
        String path = "/StopPoint/" + seg;
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url(path, null)))
                .timeout(Duration.ofSeconds(45))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() == 404) {
            return Optional.empty();
        }
        if (r.statusCode() == 429) {
            throw new RuntimeException("TfL rate limited (429): " + path);
        }
        if (r.statusCode() >= 400) {
            r.body(); // consume
            throw new IOException("TfL HTTP " + r.statusCode() + " for " + path);
        }
        JsonNode data = mapper.readTree(r.body());
        if (data != null && data.isArray() && data.size() > 0 && data.get(0).isObject()) {
            return Optional.of(data.get(0));
        }
        if (data != null && data.isObject()) {
            return Optional.of(data);
        }
        return Optional.empty();
    }

    public JsonNode fetchArrivalsForStop(String naptanId) throws IOException, InterruptedException {
        String seg = URLEncoder.encode(naptanId, StandardCharsets.UTF_8).replace("+", "%20");
        return getJson("/StopPoint/" + seg + "/Arrivals", null);
    }
}
