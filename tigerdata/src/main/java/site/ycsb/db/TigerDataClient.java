package site.ycsb.db;

import java.util.List;

import com.benchant.realtimeiot.RtIotDb;
import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;
import site.ycsb.Status;

public class TigerDataClient extends RtIotDb {

    public TigerDataClient() {
        super();
    }
    
    public Status insertMachineData(List<RtIotMachineData> machineData){
        System.err.println("Inserting batch for machines with " + machineData.size() + " entries.");
        System.err.println("\t" + machineData);
        return Status.ERROR;
    }

    public Status insertSiteData(List<RtIotSitesData> siteData){
        System.err.println("Inserting batch for sites with " + siteData.size() + " entries.");
        System.err.println("\t" + siteData);
        return Status.ERROR;
    }

    public Status insertOperatorData(List<RtIotOperatorData> operatorData) {
        System.err.println("Inserting batch for operators with " + operatorData.size() + " entries.");
        System.err.println("\t" + operatorData);
        return Status.ERROR;
    }

    public Status insertSensorData(List<RtIotSensorData> sensorData) {
        System.err.println("Inserting batch for sender with " + sensorData.size() + " entries.");
        System.err.println("\t" + sensorData);
        return Status.ERROR;
    }

}
