package com.londontransport.flink;

import java.io.Serializable;

/** One TfL arrival row after parsing (poll stop, vehicle, line, TTS seconds, bearing or -1). */
public final class Arrival implements Serializable {
    private static final long serialVersionUID = 1L;

    public String pollStop;
    public String vehicleId;
    public String lineName;
    public int timeToStationSecs;
    public float bearing;

    public Arrival() {}

    public Arrival(String pollStop, String vehicleId, String lineName, int timeToStationSecs, float bearing) {
        this.pollStop = pollStop;
        this.vehicleId = vehicleId;
        this.lineName = lineName;
        this.timeToStationSecs = timeToStationSecs;
        this.bearing = bearing;
    }
}
