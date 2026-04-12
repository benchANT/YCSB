package com.benchant.realtimeiot;

import java.util.concurrent.atomic.AtomicLong;

final class RIotSenderGroupMetaData {
    int machinesInGroup;
    int senderId;
    int firstMachineId;
    AtomicLong epoch = new AtomicLong(0);

    RIotSenderGroupMetaData(int senderId, int machinesInGroup, int firstMachineId) {
        this.senderId = senderId;
        this.machinesInGroup = machinesInGroup;
        this.firstMachineId = firstMachineId;
    }
}
