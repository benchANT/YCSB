package com.benchant.realtimeiot.handler;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.benchant.realtimeiot.data.RtIotSitesData;

import site.ycsb.RandomByteIterator;
import site.ycsb.generator.UniformLongGenerator;

public final class RtIotSitesGeneratorHandler {
    
    public static final int SITES_BATCH_SIZE = 100;

    private final CountDownLatch latch;
    private final int totalSites;
    private final AtomicInteger siteIdCounter = new AtomicInteger(0);
    private final UniformLongGenerator addressLengthGenerator = new UniformLongGenerator(10, 25);

    public RtIotSitesGeneratorHandler(int totalSites) {
        this(totalSites, 1);
    }

    public RtIotSitesGeneratorHandler(int totalSites, int threadcount) {
        this.totalSites = totalSites;
        this.latch = new CountDownLatch(threadcount);
    }

    public int count() {
        return siteIdCounter.get();
    }

    public boolean isDone() {
        return siteIdCounter.get() >= totalSites;
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

    public boolean generateSitesBatch(List<RtIotSitesData> batchEntries) {
        int maxSiteId = siteIdCounter.addAndGet(SITES_BATCH_SIZE);
        for (int i = 0; i < SITES_BATCH_SIZE; i++) {
            int siteId = maxSiteId - SITES_BATCH_SIZE + i;
            if(siteId > totalSites) {
                break;
            }
            batchEntries.add(generateSitesEntry(siteId));
        }
        return batchEntries.size() == 0;
    }

    private RtIotSitesData generateSitesEntry(int siteId) {
        long startTime = (long)(Math.random() * 1000000000);
        return new RtIotSitesData()
            .withSiteId(siteId)
            .withAddress("Address" + new RandomByteIterator(addressLengthGenerator.nextValue().longValue()))
            .withIsActive(Math.random() < 0.8)
            .withConstructionBegin(startTime)
            .withConstructionEnd(startTime + (long)(Math.random() * 1000000000));
    }
}
