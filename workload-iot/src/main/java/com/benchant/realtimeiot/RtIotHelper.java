package com.benchant.realtimeiot;

import site.ycsb.generator.DiscreteGenerator;

public final class RtIotHelper {
    private RtIotHelper() {
        // private constructor to prevent instantiation
    }

    public static DiscreteGenerator initializeMachineStatusGenerator() {
        DiscreteGenerator generator = new DiscreteGenerator();
        generator.addValue(0.5, "Off");
        generator.addValue(0.3, "Idle");
        generator.addValue(0.2, "Working");
        return generator;
    }

    static DiscreteGenerator initializeEngineStateGenerator() {
        DiscreteGenerator generator = new DiscreteGenerator();
        generator.addValue(0.5, "Off");
        generator.addValue(0.3, "Idle");
        generator.addValue(0.2, "Working");
        return generator;
    }

    static float generateLatitude() {
        // Generate a latitude value between -90 and 90
        return (float) (Math.random() * 180 - 90);
    }

    static float generateLongitude() {
        // Generate a longitude value between -180 and 180
        return (float) (Math.random() * 360 - 180);
    }

    static float generateCoolantTemperature() {
        // Generate a coolant temperature value between 10 and 150
        return (float) (Math.random() * 140 + 10);
    }

    static float generateHydraulicOilTemperature() {
        // Generate a hydraulic oil temperature value between 10 and 50
        return (float) (Math.random() * 40 + 10);
    }

}
