package com.benchant.realtimeiot;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.htrace.core.Tracer;

import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;
import com.benchant.realtimeiot.handler.RtIotMachinesGeneratorHandler;
import com.benchant.realtimeiot.handler.RtIotOperatorGeneratorHandler;
import com.benchant.realtimeiot.handler.RtIotSitesGeneratorHandler;

import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.UniformLongGenerator;

public final class RtIotWorkload extends Workload<RtIotDb> {

    // state of the workload
    private RtIotConfiguration configuration;
    private long requiredTimeSteps;
    private final AtomicLong operationCount = new AtomicLong(0);
    private NumberGenerator siteGenerator;
    private NumberGenerator operatorGenerator;
    private RtIotSitesGeneratorHandler sitesInitHandler;
    private RtIotOperatorGeneratorHandler operatorInitHandler;
    private RtIotMachinesGeneratorHandler machinesInitHandler;
    private final DiscreteGenerator engineStateGenerator = RtIotHelper.initializeEngineStateGenerator();
    private final UniformLongGenerator engineRpNumberGenerator = new UniformLongGenerator(0, 5000);
    private final UniformLongGenerator fuelLevelPercentGenerator = new UniformLongGenerator(0, 10000);
    private final UniformLongGenerator coolantTemperatureGenerator = new UniformLongGenerator(1000, 15000);
    private final UniformLongGenerator hydraulicOilTemperatureGenerator = new UniformLongGenerator(1000, 50000);
    private final List<RIotSenderGroupMetaData> senderGroups = new ArrayList<>();

    @Override
    public RtIotDb createDbWrapper(RtIotDb db, Tracer tracer) {
        return new RtIotDBWrapper(db, tracer);
    }

    @Override
    public Class<RtIotDb> getDBClass() {
        return RtIotDb.class;
    }

    @Override
    public void init(Properties properties) throws WorkloadException {
        // Initialize workload-specific properties here
        super.init(properties);

        configuration = new RtIotConfiguration(properties);

        // now we compute how many machines we get per sender and store the overall information in a list of type RIotSenderGroupMetaData
        int machinesPerSender = configuration.machines / configuration.senders;
        int remainingMachines = configuration.machines % configuration.senders;
        if(remainingMachines > 0) {
            System.err.println("Warning: Machines cannot be evenly distributed among senders. " + remainingMachines + " machines will be distributed among the first " + remainingMachines + " senders.");
        }

        int firstMachineId = 0;
        for (int i = 0; i < configuration.senders; i++) {
            int groupMachines = machinesPerSender + (i < remainingMachines ? 1 : 0);
            senderGroups.add(new RIotSenderGroupMetaData(i, groupMachines, firstMachineId));
            firstMachineId += groupMachines;
        }

        // configuration.senderFrequency tells us the interval between two batches of entries for a sender. We can use this information to compute how many entries we need to create per batch and sender to achieve the required frequency.
        int totalSeconds = (int) (configuration.stopTime.getEpochSecond() - configuration.startTime.getEpochSecond());
        int totalBatchesRemainder = totalSeconds % configuration.senderFrequency;
        if(totalBatchesRemainder > 0) {
            System.err.println("Warning: Total time interval ( " + totalSeconds + " )cannot be evenly divided by sender frequency. We will add one iteration and go beyond the specified time interval. ");
        }
        this.requiredTimeSteps = (totalSeconds / configuration.senderFrequency) + (totalBatchesRemainder > 0 ? 1 : 0);
        this.siteGenerator = new UniformLongGenerator(0, configuration.sites - 1);
        this.operatorGenerator = new UniformLongGenerator(0, configuration.operators - 1);
        this.operatorInitHandler = new RtIotOperatorGeneratorHandler(configuration.operators);
        this.sitesInitHandler = new RtIotSitesGeneratorHandler(configuration.sites);
        this.machinesInitHandler = new RtIotMachinesGeneratorHandler(configuration.machines);
    }
    
    @Override
    public void cleanup() throws WorkloadException {
        super.cleanup();
    }

    /*
     doTransactions is called during the RUN phase of the workload. It will 
     create a new batch of entries to be added depending on the number of 
     configured sensors, senders, and the required time interval.
     */
    @Override
    public boolean doTransaction(RtIotDb db, Object threadState) {
        throw new UnsupportedOperationException("RUN phase is currently not supported in this workload. It is planned for version 2.0.");
    }

    /*
        doInsert is called during the LOAD phase of the workload. It will create
        a new batch of entries to be added depending on the number of configured
        sensors, senders, and the required time interval. It loads data as fast as
        and does consider real-time behaviour.
    */
    @Override
    public boolean doInsert(RtIotDb db, Object threadState) {
        if(!configuration.skipOperators && doOperatorInsert(db, threadState)) {
            return true;
        }
        if(!configuration.skipSites && doSitesInsert(db, threadState)) {
            return true;
        }
        if(!configuration.skipMachines && doMachinesInsert(db, threadState)) {
            return true;
        }
        return doTelemetryInsert(db, threadState);
    }

    private boolean doMachinesInsert(RtIotDb db, Object threadState) {
        System.err.println("Checking if we need to insert machines. Current count: " + machinesInitHandler.count());
        if(machinesInitHandler.isDone()) {
            // we may need a countdown latch here.
            return false;
        }
        List<RtIotMachineData> batchEntries = new ArrayList<>();
        boolean done = machinesInitHandler.generateMachinesBatch(batchEntries);
        if(done) {
            return false;
        }
        // System.err.println("Inserting batch for machines with " + batchEntries.size() + " entries.");
        // System.err.println("\t" + batchEntries);
        db.insertMachineData(batchEntries);
        return true;
    }

    private boolean doSitesInsert(RtIotDb db, Object threadState) {
        System.err.println("Checking if we need to insert sites. Current count: " + sitesInitHandler.count());
        if(sitesInitHandler.isDone()) {
            // we may need a countdown latch here.
            return false;
        }
        List<RtIotSitesData> batchEntries = new ArrayList<>();
        boolean done = sitesInitHandler.generateSitesBatch(batchEntries);
        if(done) {
            return false;
        }
        // System.err.println("Inserting batch for sites with " + batchEntries.size() + " entries.");
        // System.err.println("\t" + batchEntries);
        db.insertSiteData(batchEntries);
        return false;
    }

    private boolean doOperatorInsert(RtIotDb db, Object threadState) {
        System.err.println("Checking if we need to insert operators. Current count: " + operatorInitHandler.count());
        if(operatorInitHandler.isDone()) {
            // we may need a countdown latch here.
            return false;
        }
        List<RtIotOperatorData> batchEntries = new ArrayList<>();
        boolean done = operatorInitHandler.generateOperatorBatch(batchEntries);
        if(done) {
            return false;
        }
        // System.err.println("Inserting batch for operators with " + batchEntries.size() + " entries.");
        // System.err.println("\t" + batchEntries);
        db.insertOperatorData(batchEntries);
        return true;

    }

    private boolean doTelemetryInsert(RtIotDb db, Object threadState) {
        List<RtIotSensorData> batchEntries = new ArrayList<>();
        long operationId = operationCount.getAndIncrement();
        // map operationId to senderId
        long senderId = operationId % configuration.senders;
        boolean done = generateSenderBatch(senderId, batchEntries);
        if(done) {
            return false;
        }
        // System.err.println("Inserting batch for sender " + senderId + " with " + batchEntries.size() + " entries.");
        // System.err.println("\t" + batchEntries);
        db.insertSensorData(batchEntries);
        return true;
    }

    private boolean generateSenderBatch(long senderId, List<RtIotSensorData> batchEntries) {
        int siteId = siteGenerator.nextValue().intValue();
        // this is a very simple approach: we pick the next sender group to be processed
        // then we generate one entry per machine in the sender group for the start time
        // and so on until we have generated enough for one sender interval. 
        RIotSenderGroupMetaData senderGroupMetaData = senderGroups.get((int) senderId);
        long currentEpoch = senderGroupMetaData.epoch.getAndIncrement();
        if(currentEpoch >= requiredTimeSteps) {
            return true;
        }
        long duration = currentEpoch * configuration.senderFrequency;
        long batchStartTimestamp = duration + configuration.startTime.getEpochSecond();
        // as sender frequency is not necessarily a multiple of machine frequency, the first timestamp for this batch 
        // may have an offset. 
        long startTimestamp = (duration % configuration.machineFrequency) == 0
            ? batchStartTimestamp
            : batchStartTimestamp + configuration.machineFrequency - (duration % configuration.machineFrequency);
        long endTimestamp = batchStartTimestamp + configuration.senderFrequency;
        for(long timestamp = startTimestamp; timestamp < endTimestamp; timestamp = timestamp + configuration.machineFrequency) {        
            // generate entries for the same timestamp
            for(int i = 0; i < senderGroupMetaData.machinesInGroup; i++) {
                long machineId = senderGroupMetaData.firstMachineId + i;
                batchEntries.add(
                    generateMachineEntry(machineId, senderId, siteId, timestamp)
                );
            }
        }
        return false;
    }

    private RtIotSensorData generateMachineEntry(long machineId, long senderId, int siteId, long timestamp) {
        return new RtIotSensorData()
            .withMachineId(machineId)
            .withSiteId(siteId)
            .withOperatorId(operatorGenerator.nextValue().longValue())
            .withTimestamp(timestamp)
            .withLatitude(RtIotHelper.generateLatitude())
            .withLongitude(RtIotHelper.generateLongitude())
            .withEngineState(engineStateGenerator.nextValue())
            .withFuelLevelPercent(fuelLevelPercentGenerator.nextValue().floatValue() / 100.0f) // convert to percentage
            .withEngineRpm(engineRpNumberGenerator.nextValue().intValue())
            .withCoolantTemperature(coolantTemperatureGenerator.nextValue().floatValue() / 100.0f)
            .withHydraulicOilTemperature(hydraulicOilTemperatureGenerator.nextValue().floatValue() / 100.0f);
    }   
}