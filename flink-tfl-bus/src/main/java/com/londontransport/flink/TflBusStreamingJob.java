package com.londontransport.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TfL tick → arrivals for all bus stops in {@code hub_stops} → ghost enrichment → PostGIS.
 */
public final class TflBusStreamingJob {

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromEnv();
        if (params.tflAppKey.isEmpty()) {
            System.err.println("TFL_APP_KEY is required (e.g. from .env.local / Docker env).");
            System.exit(1);
        }

        int n = HubSync.syncHubStops(params);
        System.out.println("Bus stops synced: " + n);
        if (n <= 0) {
            System.err.println(
                    "No bus stops were loaded after TfL StopPoint/Type and Search fallbacks. "
                            + "Verify TFL_APP_KEY (and optional TFL_APP_ID) and that api.tfl.gov.uk is reachable.");
            System.exit(1);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(8, Time.seconds(20)));

        TypeInformation<Arrival> arrivalType = TypeInformation.of(Arrival.class);
        TypeInformation<BusPosition> positionType = TypeInformation.of(BusPosition.class);

        DataStream<String> ticks =
                env.socketTextStream(JobParams.tickHost(), JobParams.tickPort(), "\n")
                        .name("tick-socket")
                        .setParallelism(1);

        DataStream<Arrival> arrivals =
                ticks.flatMap(new PollArrivalsFlatMap(params), arrivalType)
                        .name("tfl-hub-arrivals")
                        .setParallelism(1);

        DataStream<BusPosition> positions =
                arrivals.keyBy(a -> a.vehicleId + "|" + a.lineName)
                        .process(new GhostEnrichFunction(params), positionType)
                        .name("ghost-enrich");

        positions.addSink(new PostgisSink(params)).name("postgis-sink").setParallelism(1);

        env.execute("tfl-bus-hub-arrivals-java");
    }
}
