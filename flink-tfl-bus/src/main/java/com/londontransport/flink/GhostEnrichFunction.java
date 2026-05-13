package com.londontransport.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Stateful ghost interpolation (mirrors Python {@code GhostEnrichKeyed}).
 *
 * <p>Hub map: lat at index 0, lon at index 1 (same as Python {@code (lat, lon)} tuple).
 */
public final class GhostEnrichFunction extends KeyedProcessFunction<String, Arrival, BusPosition> {

    private static final long serialVersionUID = 1L;

    private final JobParams params;
    private transient Map<String, double[]> hubs;
    private transient ValueState<String> ghostState;
    private transient ObjectMapper mapper;

    public GhostEnrichFunction(JobParams params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.hubs = HubRepository.loadHubLatLonMap(params);
        ValueStateDescriptor<String> desc = new ValueStateDescriptor<>("ghost_json", Types.STRING);
        this.ghostState = getRuntimeContext().getState(desc);
        this.mapper = new ObjectMapper();
    }

    private static double lerp(double a, double b, double t) {
        return a + (b - a) * t;
    }

    @Override
    public void processElement(Arrival value, Context ctx, Collector<BusPosition> out) throws Exception {
        String poll = value.pollStop;
        double[] nextH = hubs.get(poll);
        if (nextH == null) {
            return;
        }

        String raw = ghostState.value();
        JsonNode stNode = (raw != null && !raw.isEmpty()) ? mapper.readTree(raw) : null;
        GhostLeg leg = GhostLeg.fromJson(stNode);

        String nextN = poll;
        String legNext = leg.next;

        if (legNext == null || !legNext.equals(nextN)) {
            leg.prev = legNext;
            leg.next = nextN;
            leg.legT0 = Math.max(value.timeToStationSecs, 60);
        } else {
            leg.legT0 = Math.max(leg.legT0, value.timeToStationSecs);
        }

        double tSeg = Math.max(leg.legT0, (double) params.ghostDefaultSegmentSecs);
        double ttsF = Math.max(value.timeToStationSecs, 0.0);
        float fraction = (float) (1.0 - Math.min(1.0, ttsF / tSeg));

        String prevId = leg.prev;
        double[] prevCoord = null;
        if (prevId != null) {
            prevCoord = hubs.get(prevId);
        }
        double nlat = nextH[0];
        double nlon = nextH[1];
        double lat;
        double lon;
        if (prevCoord != null) {
            double plat = prevCoord[0];
            double plon = prevCoord[1];
            lat = lerp(plat, nlat, fraction);
            lon = lerp(plon, nlon, fraction);
        } else {
            lat = nlat;
            lon = nlon;
        }

        ghostState.update(mapper.writeValueAsString(leg.toJson(mapper)));

        String prevOut = prevId != null ? prevId : "";
        long eventMs = System.currentTimeMillis();
        out.collect(
                new BusPosition(
                        eventMs,
                        value.vehicleId,
                        value.lineName,
                        lon,
                        lat,
                        value.bearing,
                        nextN,
                        prevOut,
                        value.timeToStationSecs,
                        fraction));
    }

    private static final class GhostLeg {
        String next;
        String prev;
        double legT0;

        static GhostLeg fromJson(JsonNode st) {
            GhostLeg g = new GhostLeg();
            if (st == null || !st.isObject()) {
                return g;
            }
            JsonNode n = st.get("next");
            JsonNode p = st.get("prev");
            JsonNode t0 = st.get("leg_t0");
            if (n != null && !n.isNull()) {
                g.next = n.asText();
            }
            if (p != null && !p.isNull()) {
                g.prev = p.asText();
            }
            if (t0 != null && t0.isNumber()) {
                g.legT0 = t0.asDouble();
            }
            return g;
        }

        JsonNode toJson(ObjectMapper mapper) {
            ObjectNode o = mapper.createObjectNode();
            if (next != null) {
                o.put("next", next);
            } else {
                o.putNull("next");
            }
            if (prev != null) {
                o.put("prev", prev);
            } else {
                o.putNull("prev");
            }
            o.put("leg_t0", legT0);
            return o;
        }
    }
}
