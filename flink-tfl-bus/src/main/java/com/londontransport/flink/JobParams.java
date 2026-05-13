package com.londontransport.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Environment / JDBC settings (mirrors Python {@code JobConfig}). */
public final class JobParams implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern JDBC_PG =
            Pattern.compile("jdbc:postgresql://([^:/]+)(?::(\\d+))?/([a-zA-Z0-9_]+)");

    public final String tflAppKey;
    /** TfL app id, or {@code null} if unset (Flink serializes {@code JobParams}; do not use {@code Optional}). */
    public final String tflAppId;
    public final List<String> hubStopTypes;
    public final String jdbcUrl;
    public final String jdbcUser;
    public final String jdbcPassword;
    public final int ghostDefaultSegmentSecs;
    public final int pollIntervalMs;
    /** Concurrent TfL /Arrivals HTTP calls per tick (large stop lists need >1). */
    public final int arrivalPollParallelism;

    public final String pgHost;
    public final int pgPort;
    public final String pgDatabase;

    public JobParams(
            String tflAppKey,
            String tflAppId,
            List<String> hubStopTypes,
            String jdbcUrl,
            String jdbcUser,
            String jdbcPassword,
            int ghostDefaultSegmentSecs,
            int pollIntervalMs,
            int arrivalPollParallelism) {
        this.tflAppKey = tflAppKey;
        this.tflAppId = tflAppId;
        this.hubStopTypes = hubStopTypes;
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.ghostDefaultSegmentSecs = ghostDefaultSegmentSecs;
        this.pollIntervalMs = pollIntervalMs;
        this.arrivalPollParallelism = arrivalPollParallelism;
        Matcher m = JDBC_PG.matcher(jdbcUrl.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException("Unsupported JDBC URL: " + jdbcUrl);
        }
        this.pgHost = m.group(1);
        this.pgPort = m.group(2) != null ? Integer.parseInt(m.group(2)) : 5432;
        this.pgDatabase = m.group(3);
    }

    public static JobParams fromEnv() {
        String rawTypes = System.getenv().getOrDefault("TFL_HUB_STOP_TYPES", "").trim();
        List<String> types = new ArrayList<>();
        if (!rawTypes.isEmpty()) {
            for (String t : rawTypes.split(",")) {
                String s = t.trim();
                if (!s.isEmpty()) {
                    types.add(s);
                }
            }
        }
        String appId = System.getenv().getOrDefault("TFL_APP_ID", "").trim();
        int arrivalParallelism =
                Math.min(
                        64,
                        Math.max(
                                1,
                                Integer.parseInt(
                                        System.getenv().getOrDefault("TFL_ARRIVAL_POLL_PARALLELISM", "32"))));
        return new JobParams(
                System.getenv().getOrDefault("TFL_APP_KEY", "").trim(),
                appId.isEmpty() ? null : appId,
                Collections.unmodifiableList(types),
                System.getenv().getOrDefault("FLINK_JDBC_URL", "jdbc:postgresql://localhost:5432/tfl_bus"),
                System.getenv().getOrDefault("JDBC_USER", "tfl"),
                System.getenv().getOrDefault("JDBC_PASSWORD", "tfl_secret"),
                Integer.parseInt(System.getenv().getOrDefault("TFL_GHOST_DEFAULT_SEGMENT_SECS", "360")),
                Integer.parseInt(System.getenv().getOrDefault("TFL_POLL_INTERVAL_MS", "30000")),
                arrivalParallelism);
    }

    public String jdbcUrlForDriver() {
        return jdbcUrl;
    }

    public static String tickHost() {
        return System.getenv().getOrDefault("TFL_TICK_HOST", "tick-feed");
    }

    public static int tickPort() {
        return Integer.parseInt(System.getenv().getOrDefault("TFL_TICK_PORT", "9999"));
    }

    public static List<String> searchQueries() {
        return Arrays.asList(
                "bus station",
                "coach station",
                "bus interchange",
                "stratford bus",
                "victoria coach",
                "heathrow bus");
    }

    /**
     * When {@code TFL_HUB_STOP_TYPES} is unset, TfL {@code Meta/StopTypes} entries to treat as bus
     * stops for sync: any id that clearly belongs to the bus/coach NaPTAN family, plus major
     * interchanges that may not contain the substring {@code "bus"}.
     */
    public static boolean metaTypeMatchesAnyBusStop(String stopType) {
        if (stopType == null) {
            return false;
        }
        String s = stopType.toLowerCase(Locale.ROOT);
        if (s.contains("bus")) {
            return true;
        }
        return s.equals("transportinterchange");
    }
}
