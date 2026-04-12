package com.benchant.realtimeiot.data;

public final class RtIotMachineData {
    
    private String vin;
    private String make;
    private String model;
    private float weightClassTons;
    private long purchaseDate;
    private String currentStatus;

    public RtIotMachineData withVin(String vin) {
        this.vin = vin;
        return this;
    }

    public RtIotMachineData withMake(String make) {
        this.make = make;
        return this;
    }

    public RtIotMachineData withModel(String model) {
        this.model = model;
        return this;
    }

    public RtIotMachineData withWeightClassTons(float weightClassTons) {
        this.weightClassTons = weightClassTons;
        return this;
    }

    public RtIotMachineData withPurchaseDate(long purchaseDate) {
        this.purchaseDate = purchaseDate;
        return this;
    }

    public RtIotMachineData withCurrentStatus(String currentStatus) {
        this.currentStatus = currentStatus;
        return this;
    }
}
