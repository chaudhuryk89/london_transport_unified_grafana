package com.londontransport.flink;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** Parses TfL /Arrivals JSON (mirrors Python {@code parse_arrivals}). */
public final class ArrivalParser {

    private ArrivalParser() {}

    public static List<Arrival> parseArrivals(String pollStopNaptan, JsonNode root) {
        List<Arrival> out = new ArrayList<>();
        if (root == null || !root.isArray()) {
            return out;
        }
        for (JsonNode p : root) {
            if (!p.isObject()) {
                continue;
            }
            JsonNode vidNode = p.get("vehicleId");
            if (vidNode == null || vidNode.isNull() || vidNode.asText().isEmpty()) {
                continue;
            }
            String vid = vidNode.asText();
            JsonNode lineName = p.get("lineName");
            JsonNode lineId = p.get("lineId");
            String line =
                    lineName != null && !lineName.isNull()
                            ? lineName.asText()
                            : (lineId != null && !lineId.isNull() ? lineId.asText() : "?");
            int tts = 0;
            JsonNode ttsNode = p.get("timeToStation");
            if (ttsNode != null && ttsNode.isNumber()) {
                tts = ttsNode.asInt();
            }
            float bearing = -1f;
            JsonNode b = p.get("bearing");
            if (b != null && b.isNumber()) {
                bearing = (float) b.asDouble();
            }
            out.add(new Arrival(pollStopNaptan, vid, line, Math.max(0, tts), bearing));
        }
        return out;
    }
}
