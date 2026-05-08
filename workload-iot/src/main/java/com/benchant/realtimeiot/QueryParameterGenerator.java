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

    public QueryParameterGenerator(Properties properties, RtIotConfiguration configuration) throws WorkloadException {
        this.configuration = configuration;
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
                // query03 does not have any parameters
                break;
            case "query04":
                generateQuery04Parameters(params);
                break;
            case "query05":
                throw new IllegalAccessError("Query05 not specified");
                // break;
            case "query06":
                generateQuery06Parameters(params);
                break;
            case "query07":
                generateQuery07Parameters(params);
                break;
            case "query08":
                generateQuery08Parameters(params);
                break;
            case "query09":
                generateQuery09Parameters(params);
                break;
            case "query10":
                generateQuery10Parameters(params);
                break;
            case "query11":
                generateQuery11Parameters(params);
                break;
            case "query12":
                generateQuery12Parameters(params);
                break;
            case "query13":
                throw new IllegalAccessError("Query13 not specified");
                // break;
            case "query14":
                generateQuery14Parameters(params);
                break;
            case "query15":
                generateQuery15Parameters(params);
                break;
            case "query16":
                generateQuery16Parameters(params);
                break;
            case "query17":
                generateQuery17Parameters(params);
                break;
            case "query18":
                generateQuery18Parameters(params);
                break;
            case "query19":
                throw new IllegalArgumentException("query19 not specified");
            case "query20":
                throw new IllegalArgumentException("query20 not specified");
            case "query21":
                generateQuery21Parameters(params);
                break;
            case "query22":
                throw new IllegalArgumentException("query22 not specified");
            case "query23":
                throw new IllegalArgumentException("query23 not specified");
            case "query24":
                throw new IllegalArgumentException("query24 not specified");
            case "query25":
                generateQuery25Parameters(params);
                break;
            case "query26":
                throw new IllegalArgumentException("query26 not specified");
            case "query27":
                throw new IllegalArgumentException("query27 not specified");
            case "query28":
                generateQuery28Parameters(params);
                break;
            case "query29":
                throw new IllegalArgumentException("query29 not specified");
            case "query30":
                throw new IllegalArgumentException("query30 not specified");
            case "query31":
                throw new IllegalArgumentException("query31 not specified");
            case "query32":
                throw new IllegalArgumentException("query32 not specified");
            case "query33":
                throw new IllegalArgumentException("query33 not specified");
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

    private void generateQuery04Parameters(Map<String, Object> params) {
        params.put(RIotParameterConstants.MACHINE_ID, RIotParameterConstants.longToMachineId(vinGenerator.nextValue().intValue()));
        nowOrStopTime(params);
    }

    private void generateQuery06Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery07Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery08Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery09Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery10Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery11Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery12Parameters(Map<String, Object> params) {
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

    private void generateQuery17Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery18Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery21Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery25Parameters(Map<String, Object> params) {
        nowOrStopTime(params);
    }

    private void generateQuery28Parameters(Map<String, Object> params) {
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
