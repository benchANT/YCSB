package com.benchant.realtimeiot.handler;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.benchant.realtimeiot.RtIotHelper;
import com.benchant.realtimeiot.data.RtIotMachineData;

import site.ycsb.RandomByteIterator;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.generator.UniformLongGenerator;

public final class RtIotMachinesGeneratorHandler {

    public static final int MACHINE_BATCH_SIZE = 100;
    
    private final CountDownLatch latch;
    private final int totalMachines;
    private final DiscreteGenerator statusGenerator = RtIotHelper.initializeMachineStatusGenerator();
    private final AtomicInteger machinesIdCounter = new AtomicInteger(0);
    private final UniformLongGenerator nameLengthGenerator = new UniformLongGenerator(3, 11);

    public RtIotMachinesGeneratorHandler(int totalMachines) {
        this(totalMachines, 1);
    }

    public RtIotMachinesGeneratorHandler(int totalMachines, int threadcount) {
        this.totalMachines = totalMachines;
        this.latch = new CountDownLatch(threadcount);
    }

    public boolean isDone() {
        return machinesIdCounter.get() >= totalMachines;
    }

    public void waitForEveryoneDone() {
        latch.countDown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            System.err.println(Thread.currentThread().getName() + "Interrupted while waiting for operator insert to complete.");
            Thread.currentThread().interrupt();
        }
    }

    public int count() {
        return machinesIdCounter.get();
    }

    public boolean generateMachinesBatch(List<RtIotMachineData> batchEntries) {
        int maxMachineId = machinesIdCounter.addAndGet(MACHINE_BATCH_SIZE);
        for (int i = 0; i < MACHINE_BATCH_SIZE; i++) {
            int machineId = maxMachineId - MACHINE_BATCH_SIZE + i;
            if(machineId > totalMachines) {
                break;
            }
            batchEntries.add(generateMachineEntry(machineId));
        }
        return batchEntries.size() == 0;
    }

    private RtIotMachineData generateMachineEntry(long machineId) {
        long purchaseDate = (long)(Math.random() * 1000000000);
        return new RtIotMachineData()
            .withVin("VIN-" + machineId)
            .withMake(new RandomByteIterator(nameLengthGenerator.nextValue().longValue()).toString())
            .withModel(new RandomByteIterator(nameLengthGenerator.nextValue().longValue()).toString())
            .withWeightClassTons((float)(Math.random() * 100))
            .withPurchaseDate(purchaseDate)
            .withCurrentStatus(statusGenerator.nextValue().toString());
     }
}
