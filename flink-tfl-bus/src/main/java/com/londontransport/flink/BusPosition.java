package com.londontransport.flink;

import java.io.Serializable;

/** Enriched bus position for PostGIS sink. */
public final class BusPosition implements Serializable {
    private static final long serialVersionUID = 1L;

    public long eventMs;
    public String vehicleId;
    public String route;
    public double lon;
    public double lat;
    public float bearing;
    public String nextNaptan;
    public String prevNaptan;
    public int rawTts;
    public float ghostFrac;

    public BusPosition() {}

    public BusPosition(
            long eventMs,
            String vehicleId,
            String route,
            double lon,
            double lat,
            float bearing,
            String nextNaptan,
            String prevNaptan,
            int rawTts,
            float ghostFrac) {
        this.eventMs = eventMs;
        this.vehicleId = vehicleId;
        this.route = route;
        this.lon = lon;
        this.lat = lat;
        this.bearing = bearing;
        this.nextNaptan = nextNaptan;
        this.prevNaptan = prevNaptan;
        this.rawTts = rawTts;
        this.ghostFrac = ghostFrac;
    }
}
