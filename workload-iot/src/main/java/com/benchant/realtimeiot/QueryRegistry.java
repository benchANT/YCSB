package com.benchant.realtimeiot;

import java.util.HashMap;
import java.util.Map;

import site.ycsb.Status;
import site.ycsb.measurements.Measurements;

public enum QueryRegistry {
    INSTANCE;

    private final Measurements measurements = Measurements.getMeasurements();
    private final Map<String,String> queries = new HashMap<>();

    public String getQuery(String queryId) {
        return queries.get(queryId);
    }

    public void addQuery(String queryId, String query) {
        String old = queries.put(queryId, query);
        if(old != null) {
            System.err.println("QueryRegistry [Warning]: queryId '" + query + "'  used more than once.");
        }
    }

    public void registerServerLatency(Status status, int latency) {
        String appendix = "";
        if(status != Status.OK) {
            appendix = "-" + status;
        }
        measurements.measure("SERVER"+appendix, latency);
    }
}
