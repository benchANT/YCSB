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

    public QueryParameterGenerator(Properties properties) throws WorkloadException {
        configuration = new RtIotConfiguration(properties);
        this.siteGenerator = new UniformLongGenerator(0, configuration.sites - 1);
    }

    public Map<String, Object> generateParameters(String queryId) {
        Map<String, Object> params = new HashMap<>();
        switch (queryId) {
            case "query01":
                generateQuery01Parameters(params);
                break;
            default:
                // otherwise now we just return an empty map
                break;
        }
        return params;
    }

    private void generateQuery01Parameters(Map<String, Object> params) {
        params.put(RIotParameterConstants.SITE_ID, siteGenerator.nextValue().intValue());
        if(configuration.stopTime != null) {
            // for now we just use the stop time as the query time, but in the future we can add some logic here to generate a random time between start and stop time
            params.put(RIotParameterConstants.TIMESTAMP_UPPER, configuration.stopTime);
        } else {
            // if no stop time is configured, we hand over null which means "now()"
            params.put(RIotParameterConstants.TIMESTAMP_UPPER, null);
        }
    }
}
