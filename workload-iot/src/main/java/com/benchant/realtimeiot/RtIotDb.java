package com.benchant.realtimeiot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;

public abstract class RtIotDb extends DB implements RIotParameterConstants {
    // Define any additional methods specific to the IoT database here
    public abstract Status insertMachineData(List<RtIotMachineData> machineData);
    public abstract Status insertSiteData(List<RtIotSitesData> siteData);
    public abstract Status insertOperatorData(List<RtIotOperatorData> operatorData);
    public abstract Status insertSensorData(List<RtIotSensorData> sensorData);
    public abstract Status query(String queryId, Map<String, Object> params, List<HashMap<String, ByteIterator>> result);
}
