package com.benchant.realtimeiot.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.benchant.realtimeiot.data.RtIotOperatorData;

import site.ycsb.RandomByteIterator;
import site.ycsb.generator.UniformLongGenerator;

public final class RtIotOperatorGeneratorHandler {

    public static final int OPERATOR_BATCH_SIZE = 100;

    private final int totalOperators;
    private final AtomicInteger operatorIdCounter = new AtomicInteger(0);
    private final UniformLongGenerator nameLengthGenerator = new UniformLongGenerator(3, 11);

    public RtIotOperatorGeneratorHandler(int totalOperators) {
        this.totalOperators = totalOperators;
    }

    public int count() {
        return operatorIdCounter.get();
    }

    public boolean isDone() {
        return operatorIdCounter.get() >= totalOperators;
    }

    public boolean generateOperatorBatch(List<RtIotOperatorData> batchEntries) {
        int maxOperatorId = operatorIdCounter.addAndGet(OPERATOR_BATCH_SIZE);
        for (int i = 0; i < OPERATOR_BATCH_SIZE; i++) {
            int operatorId = maxOperatorId - OPERATOR_BATCH_SIZE + i;
            if(operatorId > totalOperators) {
                break;
            }
            batchEntries.add(generateOperatorEntry(operatorId));
        }
        return batchEntries.size() == 0;
    }

    private RtIotOperatorData generateOperatorEntry(long operatorId) {
        return new RtIotOperatorData()
            .withOperatorId(operatorId)
            .withFirstName("FirstName" + new RandomByteIterator(nameLengthGenerator.nextValue().longValue()))
            .withLastName("LastName" + new RandomByteIterator(nameLengthGenerator.nextValue().longValue()));
     }
}
