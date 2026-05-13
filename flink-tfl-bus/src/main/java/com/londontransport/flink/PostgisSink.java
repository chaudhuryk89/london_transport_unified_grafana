package com.londontransport.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

/** JDBC insert into bus_positions with PostGIS (mirrors Python {@code PostgisBusPositionsFlatMap}). */
public final class PostgisSink extends RichSinkFunction<BusPosition> {

    private static final long serialVersionUID = 1L;

    private final JobParams params;
    private transient Connection conn;

    public PostgisSink(JobParams params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DriverManager.getConnection(params.jdbcUrlForDriver(), params.jdbcUser, params.jdbcPassword);
        conn.setAutoCommit(true);
    }

    @Override
    public void invoke(BusPosition v, Context context) throws Exception {
        String sql =
                "INSERT INTO bus_positions (time, vehicle_id, route, location, bearing, "
                        + "next_naptan_id, prev_naptan_id, raw_time_to_station, ghost_fraction) "
                        + "VALUES (?,?,?,ST_SetSRID(ST_MakePoint(?,?),4326),?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(Instant.ofEpochMilli(v.eventMs)));
            ps.setString(2, v.vehicleId);
            ps.setString(3, v.route);
            ps.setDouble(4, v.lon);
            ps.setDouble(5, v.lat);
            if (v.bearing < 0) {
                ps.setNull(6, Types.REAL);
            } else {
                ps.setFloat(6, v.bearing);
            }
            ps.setString(7, v.nextNaptan);
            if (v.prevNaptan == null || v.prevNaptan.isEmpty()) {
                ps.setNull(8, Types.VARCHAR);
            } else {
                ps.setString(8, v.prevNaptan);
            }
            ps.setInt(9, v.rawTts);
            ps.setFloat(10, v.ghostFrac);
            ps.executeUpdate();
        }
    }

    @Override
    public void close() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
        super.close();
    }
}
