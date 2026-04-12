package com.benchant.realtimeiot;

import java.util.List;

import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;

import site.ycsb.DB;
import site.ycsb.Status;

public abstract class RtIotDb extends DB {
    // Define any additional methods specific to the IoT database here
    public abstract Status insertMachineData(List<RtIotMachineData> machineData);
    public abstract Status insertSiteData(List<RtIotSitesData> siteData);
    public abstract Status insertOperatorData(List<RtIotOperatorData> operatorData);
    public abstract Status insertSensorData(List<RtIotSensorData> sensorData);
}
