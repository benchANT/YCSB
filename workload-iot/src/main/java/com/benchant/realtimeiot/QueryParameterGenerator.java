package com.benchant.realtimeiot;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import site.ycsb.WorkloadException;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.UniformLongGenerator;

public final class QueryParameterGenerator {

    private RtIotConfiguration configuration;
    private NumberGenerator siteGenerator;
    private NumberGenerator vinGenerator;

    public QueryParameterGenerator(Properties properties) throws WorkloadException {
        configuration = new RtIotConfiguration(properties);
        this.siteGenerator = new UniformLongGenerator(0, configuration.sites - 1);
        this.vinGenerator = new UniformLongGenerator(0, configuration.machines - 1);
    }

    public Map<String, Object> generateParameters(String queryId) {
        Map<String, Object> params = new HashMap<>();
        switch (queryId) {
            case "query01":
                generateQuery01Parameters(params);
                break;
            case "query02":
                generateQuery02Parameters(params);
                break;
            case "query03":
                // query03 does not have any paramaters
                break;
            case "query04":
                throw new IllegalAccessError("Query04 not supported yet");
                // break;
            case "query05":
                throw new IllegalAccessError("Query05 not supported yet");
                // break;
            case "query06":
                generateQuery06Parameters(params);
                break;
            // case query07
            // case query08
            // case query09
            // case query10
            // case query11
            // case query12
            // case query13
            case "query14":
                generateQuery14Parameters(params);
                break;
            case "query15":
                generateQuery15Parameters(params);
                break;
            case "query16":
                generateQuery16Parameters(params);
                break;
            case "query18":
                generateQuery18Parameters(params);
                break;
            case "query25":
                generateQuery25Parameters(params);
                break;
            default:
                // otherwise now we just return an empty map
                break;
        }
        return params;
    }

    private void generateQuery01Parameters(Map<String, Object> params) {
        params.put(RIotParameterConstants.SITE_ID, siteGenerator.nextValue().intValue());
        nowOrStopTime(params);
    }

    private void generateQuery02Parameters(Map<String, Object> params) {
        params.put(RIotParameterConstants.MACHINE_ID, RIotParameterConstants.longToMachineId(vinGenerator.nextValue().intValue()));
    }

    private void generateQuery06Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery14Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery15Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery16Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery18Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery25Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void nowOrStopTime(Map<String, Object> params) {
        if(configuration.stopTime != null) {
            // for now we just use the stop time as the query time, but in the future we can add some logic here to generate a random time between start and stop time
            params.put(RIotParameterConstants.TIMESTAMP_UPPER, configuration.stopTime);
        } else {
            // if no stop time is configured, we hand over null which means "now()"
            params.put(RIotParameterConstants.TIMESTAMP_UPPER, null);
        }
    }
    
}
