package com.benchant.realtimeiot;

import java.time.Instant;
import java.util.Properties;

import site.ycsb.WorkloadException;
import site.ycsb.util.PropertyParser;

public final class RtIotConfiguration {

    // Property names
    public static final String PROP_START_TIME = "riot.start_time";
    public static final String PROP_STOP_TIME = "riot.stop_time";
    public static final String PROP_MACHINES = "riot.machines";
    public static final String PROP_SENDERS = "riot.senders";
    public static final String PROP_SITES = "riot.sites";
    public static final String PROP_OPERATORS = "riot.operators";
    public static final String PROP_MACHINE_FREQUENCY = "riot.machine_frequency";
    public static final String PROP_SENDER_FREQUENCY = "riot.sender_frequency";
    public static final String SKIP_OPERATORS = "riot.skip_operators";
    public static final String SKIP_SITES = "riot.skip_sites";
    public static final String SKIP_MACHINES = "riot.skip_machines";

    // Property defaults
    public static final String DEFAULT_START_TIME = "2026-01-01T00:00:00Z";
    public static final String DEFAULT_STOP_TIME = "2026-03-31T23:59:59Z";
    public static final int DEFAULT_MACHINES = 10000;
    public static final int DEFAULT_SENDERS = 200;
    public static final int DEFAULT_OPERATORS = 1;
    public static final int DEFAULT_SITES = 1;
    public static final int DEFAULT_MACHINE_FREQUENCY = 15;
    public static final int DEFAULT_SENDER_FREQUENCY = 60;

    // Stored properties
    Instant startTime;
    Instant stopTime;
    int machines;
    int senders;
    int machineFrequency;
    int senderFrequency;
    int sites;
    int operators;

    boolean skipOperators;
    boolean skipSites;
    boolean skipMachines;

    RtIotConfiguration(Properties properties) throws WorkloadException {
        // Parse start time
        this.startTime = PropertyParser.parseInstantProperty(properties, PROP_START_TIME, Instant.parse(DEFAULT_START_TIME));
        // Parse stop time
        this.stopTime = PropertyParser.parseInstantProperty(properties, PROP_STOP_TIME, Instant.parse(DEFAULT_STOP_TIME));
        // Validate that stop time is after start time
        if (!this.stopTime.isAfter(this.startTime)) {
            throw new WorkloadException("Stop time must be after start time. Start: " + this.startTime + ", Stop: " + this.stopTime);
        }

        // Parse machines
        this.machines = PropertyParser.parseIntProperty(properties, PROP_MACHINES, DEFAULT_MACHINES);
        if (this.machines <= 0) {
            throw new WorkloadException("Machines must be greater than 0, got: " + this.machines);
        }

        // Parse senders
        this.senders = PropertyParser.parseIntProperty(properties, PROP_SENDERS, DEFAULT_SENDERS);
        if (this.senders <= 0) {
            throw new WorkloadException("Senders must be greater than 0, got: " + this.senders);
        } 

        // Parse machine frequency
        this.machineFrequency = PropertyParser.parseIntProperty(properties, PROP_MACHINE_FREQUENCY, DEFAULT_MACHINE_FREQUENCY);
        if (this.machineFrequency <= 0) {
            throw new WorkloadException("Machine frequency must be greater than 0, got: " + this.machineFrequency);
        }

        // Parse sender frequency
        this.senderFrequency = PropertyParser.parseIntProperty(properties, PROP_SENDER_FREQUENCY, DEFAULT_SENDER_FREQUENCY);
        if (this.senderFrequency <= 0) {
            throw new WorkloadException("Sender frequency must be greater than 0, got: " + this.senderFrequency);
        }

        this.sites = PropertyParser.parseIntProperty(properties, PROP_SITES, DEFAULT_SITES);
        if (this.sites <= 0) {
            throw new WorkloadException("Sites must be greater than 0, got: " + this.sites);
        }

        this.operators = PropertyParser.parseIntProperty(properties, PROP_OPERATORS, DEFAULT_OPERATORS);
        if (this.operators <= 0) {
            throw new WorkloadException("Operators must be greater than 0, got: " + this.operators);
        }
        this.skipOperators = PropertyParser.parseBooleanProperty(properties, SKIP_OPERATORS, false);
        this.skipSites = PropertyParser.parseBooleanProperty(properties, SKIP_SITES, false);
        this.skipMachines = PropertyParser.parseBooleanProperty(properties, SKIP_MACHINES, false);
        
        // log the parsed configuration
        System.err.println("Configuration:");
        System.err.println("Start Time: " + this.startTime);
        System.err.println("Stop Time: " + this.stopTime);
        System.err.println("Machines: " + this.machines);
        System.err.println("Senders: " + this.senders);
        System.err.println("Sites: " + this.sites);
        System.err.println("Operators: " + this.operators);
        System.err.println("Machine Frequency: " + this.machineFrequency);
        System.err.println("Sender Frequency: " + this.senderFrequency);
        System.err.println("Skip Operators: " + this.skipOperators);
        System.err.println("Skip Sites: " + this.skipSites);
        System.err.println("Skip Machines: " + this.skipMachines);
    }
}
