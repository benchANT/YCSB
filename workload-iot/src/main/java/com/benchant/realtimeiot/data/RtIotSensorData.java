package com.benchant.realtimeiot.data;

public final class RtIotSensorData {

    private long timestamp;
    private long machineId;
    private long operatorId;
    private long siteId;
    private float latitude;
    private float longitude;
    private String engineState;
    private float fuelLevelPercent;
    private int engineRpm;
    private float coolantTemperature;
    private float hydraulicOilTemperature;

    @Override
    public String toString() {
        return "RtIotSensorData{" +
            "timestamp=" + timestamp +
            ", machineId=" + machineId +
            ", operatorId=" + operatorId +
            ", siteId=" + siteId +
            ", latitude=" + latitude +
            ", longitude=" + longitude +
            ", engineState='" + engineState + '\'' +
            ", fuelLevelPercent=" + fuelLevelPercent +
            ", engineRpm=" + engineRpm +
            ", coolantTemperature=" + coolantTemperature +
            ", hydraulicOilTemperature=" + hydraulicOilTemperature +
            '}';
    }

    public RtIotSensorData withTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public RtIotSensorData withMachineId(long  machineId) {
      this.machineId = machineId;
      return this;
    }

    public RtIotSensorData withOperatorId(long operatorId) {
      this.operatorId = operatorId;
      return this;
    }

    public RtIotSensorData withSiteId(long siteId) {
      this.siteId = siteId;
      return this;
    }

    public RtIotSensorData withLatitude(float latitude) {
      this.latitude = latitude;
      return this;
    }

    public RtIotSensorData withLongitude(float longitude) {
      this.longitude = longitude;
      return this;
    }

    public RtIotSensorData withEngineState(String engineState) {
      this.engineState = engineState;
      return this;
    }

    public RtIotSensorData withFuelLevelPercent(float fuelLevelPercent) {
      this.fuelLevelPercent = fuelLevelPercent;
      return this;
    }

    public RtIotSensorData withEngineRpm(int engineRpm) {
      this.engineRpm = engineRpm;
      return this;
    }

    public RtIotSensorData withCoolantTemperature(float coolantTemperature) {
      this.coolantTemperature = coolantTemperature;
      return this;
    }

    public RtIotSensorData withHydraulicOilTemperature(float hydraulicOilTemperature) {
      this.hydraulicOilTemperature = hydraulicOilTemperature;
      return this;
    }
}
