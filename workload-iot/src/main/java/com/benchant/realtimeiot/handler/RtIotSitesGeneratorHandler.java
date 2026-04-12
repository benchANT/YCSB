package com.benchant.realtimeiot.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.benchant.realtimeiot.data.RtIotSitesData;

import site.ycsb.RandomByteIterator;
import site.ycsb.generator.UniformLongGenerator;

public final class RtIotSitesGeneratorHandler {
    
    public static final int SITES_BATCH_SIZE = 100;

    private final int totalSites;
    private final AtomicInteger siteIdCounter = new AtomicInteger(0);
    private final UniformLongGenerator addressLengthGenerator = new UniformLongGenerator(10, 25);

    public RtIotSitesGeneratorHandler(int totalSites) {
        this.totalSites = totalSites;
    }

    public int count() {
        return siteIdCounter.get();
    }

    public boolean isDone() {
        return siteIdCounter.get() >= totalSites;
    }

    public boolean generateSitesBatch(List<RtIotSitesData> batchEntries) {
        int maxSiteId = siteIdCounter.addAndGet(SITES_BATCH_SIZE);
        for (int i = 0; i < SITES_BATCH_SIZE; i++) {
            int siteId = maxSiteId - SITES_BATCH_SIZE + i + 1;
            if(siteId > totalSites) {
                break;
            }
            batchEntries.add(generateSitesEntry(siteId));
        }
        return batchEntries.size() == 0;
    }

    private RtIotSitesData generateSitesEntry(long siteId) {
        long startTime = (long)(Math.random() * 1000000000);
        return new RtIotSitesData()
            .withAddress("Address" + new RandomByteIterator(addressLengthGenerator.nextValue().longValue()))
            .withIsActive(Math.random() < 0.8)
            .withConstructionBegin(startTime)
            .withConstructionEnd(startTime + (long)(Math.random() * 1000000000));
    }
}
