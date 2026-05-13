package com.londontransport.flink;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** TfL bus stop discovery (all bus-related Meta types when unset) + Postgres {@code hub_stops} upsert. */
public final class HubSync {

    private HubSync() {}

    public static int syncHubStops(JobParams cfg) throws Exception {
        TflApiClient client = new TflApiClient(cfg);
        Map<String, HubRepository.HubStopRow> merged = new LinkedHashMap<>();

        if (cfg.hubStopTypes.isEmpty()) {
            syncByMode(client, merged);
        }

        if (merged.isEmpty()) {
            List<String> meta = client.fetchMetaStopTypes();
            List<String> selected = selectTypes(meta, cfg.hubStopTypes);
            if (selected.isEmpty()) {
                throw new RuntimeException(
                        "No bus stop types selected. Set TFL_HUB_STOP_TYPES or check TfL Meta/StopTypes.");
            }
            syncByTypes(client, selected, merged);
        }

        if (merged.isEmpty()) {
            searchFallback(client, merged);
        }

        HubRepository.upsertHubStops(cfg, new ArrayList<>(merged.values()));
        return merged.size();
    }

    /**
     * Primary path when {@code TFL_HUB_STOP_TYPES} is unset: page through
     * {@code /StopPoint/Mode/bus} which returns the entire London bus stop catalog
     * (~19k+ rows). TfL reports {@code total} on each response so we can stop cleanly.
     */
    private static void syncByMode(TflApiClient client, Map<String, HubRepository.HubStopRow> merged)
            throws Exception {
        int page = 1;
        int seen = 0;
        int total = -1;
        while (true) {
            TflApiClient.StopPointPage resp = client.fetchStopPointsForMode("bus", page);
            if (resp.rows.isEmpty()) {
                break;
            }
            for (JsonNode sp : resp.rows) {
                HubRepository.parseIfBusHub(sp).ifPresent(row -> merged.put(row.naptanId, row));
            }
            seen += resp.rows.size();
            if (resp.total > 0) {
                total = resp.total;
            }
            System.out.println(
                    "StopPoint/Mode bus page=" + page + " rows=" + resp.rows.size()
                            + " accepted=" + merged.size() + (total > 0 ? " total=" + total : ""));
            if (total > 0 && seen >= total) {
                break;
            }
            page++;
            Thread.sleep(120);
        }
    }

    private static void syncByTypes(
            TflApiClient client, List<String> selected, Map<String, HubRepository.HubStopRow> merged)
            throws Exception {
        for (String stopType : selected) {
            int page = 0;
            List<JsonNode> stops = client.fetchStopPointsForType(stopType, page);
            if (stops.isEmpty()) {
                page = 1;
                stops = client.fetchStopPointsForType(stopType, page);
            }
            if (stops.isEmpty()) {
                continue;
            }
            while (true) {
                for (JsonNode sp : stops) {
                    HubRepository.parseIfBusHub(sp).ifPresent(row -> merged.put(row.naptanId, row));
                }
                page++;
                stops = client.fetchStopPointsForType(stopType, page);
                if (stops.isEmpty()) {
                    break;
                }
                Thread.sleep(150);
            }
        }
    }

    private static List<String> selectTypes(List<String> metaTypes, List<String> explicit) {
        if (!explicit.isEmpty()) {
            List<String> out = explicit.stream().filter(metaTypes::contains).collect(Collectors.toList());
            return !out.isEmpty() ? out : new ArrayList<>(explicit);
        }
        List<String> out = new ArrayList<>();
        for (String t : metaTypes) {
            if (JobParams.metaTypeMatchesAnyBusStop(t)) {
                out.add(t);
            }
        }
        if (out.isEmpty()) {
            for (String cand :
                    List.of(
                            "NaptanPublicBusCoachTram",
                            "NaptanOnstreetBusCoachStop",
                            "NaptanBusCoachStation",
                            "TransportInterchange")) {
                if (metaTypes.contains(cand)) {
                    out.add(cand);
                }
            }
        }
        Collections.sort(out);
        prioritizePrimaryBusType(out);
        return out;
    }

    /** Puts {@code NaptanPublicBusCoachTram} first (bulk of London bus stops, ~19k+). */
    private static void prioritizePrimaryBusType(List<String> out) {
        int idx = out.indexOf("NaptanPublicBusCoachTram");
        if (idx > 0) {
            out.remove(idx);
            out.add(0, "NaptanPublicBusCoachTram");
        }
    }

    private static void searchFallback(TflApiClient client, Map<String, HubRepository.HubStopRow> merged)
            throws Exception {
        System.out.println("StopPoint/Type returned no usable rows; trying StopPoint/Search fallback…");
        for (String q : JobParams.searchQueries()) {
            try {
                JsonNode raw = client.searchStopPoints(q, "bus");
                mergeSearchResponse(client, raw, merged, 2500);
            } catch (Exception e) {
                System.out.println("Search fallback query '" + q + "' failed: " + e);
            }
            Thread.sleep(200);
        }
    }

    private static void mergeSearchResponse(
            TflApiClient client, JsonNode raw, Map<String, HubRepository.HubStopRow> merged, int limit)
            throws Exception {
        if (raw == null || !raw.isObject()) {
            return;
        }
        JsonNode matches = raw.get("matches");
        if (matches == null || !matches.isArray()) {
            return;
        }
        int n = 0;
        for (JsonNode m : matches) {
            if (n >= limit) {
                break;
            }
            if (!m.isObject()) {
                continue;
            }
            HubRepository.HubStopRow row = HubRepository.parseIfBusHub(m).orElse(null);
            if (row == null && m.get("id") != null) {
                JsonNode idNode = m.get("id");
                if (idNode != null && !idNode.isNull()) {
                    row = client.fetchStopPoint(idNode.asText()).flatMap(HubRepository::parseIfBusHub).orElse(null);
                }
            }
            if (row != null) {
                merged.put(row.naptanId, row);
                n++;
            }
            Thread.sleep(40);
        }
    }
}
