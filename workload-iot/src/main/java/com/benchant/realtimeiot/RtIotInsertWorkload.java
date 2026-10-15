package com.benchant.realtimeiot;

import java.util.Properties;

import org.apache.htrace.core.Tracer;

import site.ycsb.Workload;
import site.ycsb.WorkloadException;

/** second version of this workload. this time with less random behavior */
public final class RtIotInsertWorkload  extends Workload<RtIotDb> {

    private RtIotConfiguration configuration;

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
        if(configuration.stopTime == null) {
            //FIXME: at a later stage, this should be running until it is "now"
            throw new IllegalStateException("Stop time must be configured for the workload. Aborting.");
        }
        return false;
    }
}
