package com.londontransport.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Each tick triggers TfL /Arrivals polls for every row in {@code hub_stops}. Uses a bounded thread
 * pool so ~19k+ stops can complete within a 30s tick when {@code TFL_ARRIVAL_POLL_PARALLELISM} is
 * set high enough (default 32, max 64).
 */
public final class PollArrivalsFlatMap extends RichFlatMapFunction<String, Arrival> {

    private static final long serialVersionUID = 1L;

    private final JobParams params;
    private transient TflApiClient client;
    private transient List<String> hubIds;
    private transient ExecutorService pool;

    public PollArrivalsFlatMap(JobParams params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.client = new TflApiClient(params);
        this.hubIds = HubRepository.loadHubNaptanIds(params);
        if (hubIds.isEmpty()) {
            throw new IllegalStateException("hub_stops is empty; hub sync must succeed before the stream starts.");
        }
        this.pool = Executors.newFixedThreadPool(params.arrivalPollParallelism);
    }

    @Override
    public void close() throws Exception {
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(20, TimeUnit.MINUTES)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            pool = null;
        }
        super.close();
    }

    private List<Arrival> fetchOne(String hid) {
        try {
            JsonNode arr = client.fetchArrivalsForStop(hid);
            return ArrivalParser.parseArrivals(hid, arr);
        } catch (Exception e) {
            System.out.println("Arrivals poll failed for " + hid + ": " + e);
            return List.of();
        }
    }

    @Override
    public void flatMap(String tick, Collector<Arrival> out) throws Exception {
        int par = params.arrivalPollParallelism;
        for (int i = 0; i < hubIds.size(); i += par) {
            int end = Math.min(i + par, hubIds.size());
            List<Future<List<Arrival>>> batch = new ArrayList<>(end - i);
            for (int j = i; j < end; j++) {
                String hid = hubIds.get(j);
                batch.add(pool.submit(() -> fetchOne(hid)));
            }
            for (Future<List<Arrival>> f : batch) {
                for (Arrival row : f.get(120, TimeUnit.SECONDS)) {
                    out.collect(row);
                }
            }
        }
    }
}
