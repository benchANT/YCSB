package site.ycsb.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;

public class InsertHelper {
    static final String OPERATOR_KEY = "operator";
    static final String OPERATOR_INSERT_STRING = "INSERT INTO operators (operator_id, first_name, last_name) VALUES (?, ?, ?)";

    static final String SITES_KEY = "sites";
    static final String SITES_INSERT_STRING = "INSERT INTO sites (site_id, address, is_active, construction_begin, construction_end) VALUES (?, ?, ?, ?, ?)";

    static final String MACHINES_KEY = "machines";
    static final String MACHINES_INSERT_STRING = "INSERT INTO machines (vin, make, model, weight_class_tons, purchase_date, current_status) VALUES (?, ?, ?, ?, ?, ?)";

    static final String TELEMETRY_KEY = "telemetry";
    static final String TELEMETRY_INSERT_STRING = "INSERT INTO telematics_data (recorded_at, vin, operator_id, site_id, latitude, longitude, engine_state, fuel_level_percent, engine_rpm, coolant_temperature, hydraulic_oil_temperature) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    static void fillOperatorInsertStatement(PreparedStatement st, RtIotOperatorData operatorData) throws SQLException {
        st.setLong(1, operatorData.getOperatorId());
        st.setString(2, operatorData.getFirstName());
        st.setString(3, operatorData.getLastName()); 
    }

    static void fillSiteInsertStatement(PreparedStatement st, RtIotSitesData siteData) throws SQLException {
        st.setLong(1, siteData.getSiteId());
        st.setString(2, siteData.getAddress());
        st.setBoolean(3, siteData.isActive());
        st.setLong(4, siteData.getConstructionBegin());
        st.setLong(5, siteData.getConstructionEnd());
    }   

    static void fillMachineInsertStatement(PreparedStatement st, RtIotMachineData machineData) throws SQLException {
        st.setString(1, machineData.getVin());
        st.setString(2, machineData.getMake());
        st.setString(3, machineData.getModel());
        st.setFloat(4, machineData.getWeightClassTons());
        st.setLong(5, machineData.getPurchaseDate());
        st.setString(6, machineData.getCurrentStatus());
    }

    static void fillTelemetryInsertStatement(PreparedStatement st, RtIotSensorData sensorData) throws SQLException {
        st.setTimestamp(1, new Timestamp(sensorData.getTimestamp()));
        st.setString(2, sensorData.getVin());
        st.setLong(3, sensorData.getOperatorId());
        st.setLong(4, sensorData.getSiteId());
        st.setFloat(5, sensorData.getLatitude());
        st.setFloat(6, sensorData.getLongitude());
        st.setString(7, sensorData.getEngineState());
        st.setFloat(8, sensorData.getFuelLevelPercent());
        st.setInt(9, sensorData.getEngineRpm());
        st.setFloat(10, sensorData.getCoolantTemperature());
        st.setFloat(11, sensorData.getHydraulicOilTemperature());
    }

}
