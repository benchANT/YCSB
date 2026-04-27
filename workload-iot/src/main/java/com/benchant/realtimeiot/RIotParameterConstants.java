package com.benchant.realtimeiot;

public interface RIotParameterConstants {
    
    public static final String SITE_ID = "siteId";
    public static final String OPERATOR_ID = "operatorId";
    public static final String MACHINE_ID = "machineId";
    public static final String SENSOR_ID = "sensorId";
    public static final String TIMESTAMP_UPPER = "timestamp_upper";

    public static String longToMachineId(long mid) {
        return "VIN-" + mid;
    }
}
