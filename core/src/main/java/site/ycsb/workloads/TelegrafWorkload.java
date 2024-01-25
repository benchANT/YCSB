/*
 * Copyright (c) 2024 benchANT GmbH. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package site.ycsb.workloads;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.TelegrafDbIfc;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.generator.Generator;
import site.ycsb.generator.StaticSequenceGenerator;
import site.ycsb.generator.StaticSequenceGenerator.SequenceGenerationOrder;
import site.ycsb.generator.StaticSequenceGenerator.SequenceItem;

public final class TelegrafWorkload extends Workload {

    static final class Tracker {
        final Vector<HashMap<String, ByteIterator>> cells = new Vector<HashMap<String, ByteIterator>>();
        volatile OperationType operation = null;
    }

    private Generator<SequenceItem<OperationType>> operationchooser;
    private final List<OperationType> operationNames = new ArrayList<>();
    private int repetitionsPerOperation = 0;
    private Duration queryInterval = null;
    private boolean printQueryResults = false;

    private static final int RESULTS_TRACKER_LENGTH = 65536;
    private final Tracker[] results = new Tracker[RESULTS_TRACKER_LENGTH];

    public TelegrafWorkload() {
        for(int i = 0; i < RESULTS_TRACKER_LENGTH; i++) {
            results[i] = new Tracker();
        }
    }
    private Generator<SequenceItem<OperationType>> createOperationGenerator(String order) {
        final SequenceGenerationOrder theOrder = "breadth".equalsIgnoreCase(order)
            ? SequenceGenerationOrder.BREADTH_FIRST
            : SequenceGenerationOrder.DEPTH_FIRST;

        return new StaticSequenceGenerator<OperationType>(
            operationNames.toArray(new OperationType[operationNames.size()]),
            repetitionsPerOperation, theOrder);
    }
    private void initOperationNames(String namelist) {
        operationNames.clear();
        if("ALL".equalsIgnoreCase(namelist)) {
            operationNames.addAll(Arrays.asList(OperationType.values()));
            return;
        }
        String[] nameStack = namelist.split(",");
        for(String s : nameStack) {
            String q = s.trim();
            for(OperationType f : OperationType.values()) {
                if(f.id.equals(q)) {
                    if(!operationNames.contains(f)) {
                        operationNames.add(f);
                    }
                    break;
                }
            }
        }
    }
    @Override
    public void cleanup() throws WorkloadException {
        if(!printQueryResults) {
            return;
        }
        for(int i = 0; i < RESULTS_TRACKER_LENGTH; i++) {
            Tracker t = results[i];
            if(t.operation == null) continue;
            final String header = "[RESULT '" + i + "' for " + t.operation;
            System.out.println(header);
            for(Map<String, ByteIterator> map : t.cells) {
                System.out.println("\t[DUMP] " + map.toString());
            }
            System.out.println("]");
        }
    }
    @Override
    public void init(Properties p) throws WorkloadException {
        initOperationNames( p.getProperty(SUPPORTED_OPERATIONS_PROPERTY, SUPPORTED_OPERATIONS_DEFAULT) );
        System.err.println("unlocked the following operations: " + operationNames);
        final String order = p.getProperty(OPERATION_ORDER_PROPERTY, OPERATION_ORDER_DEFAULT);

        repetitionsPerOperation = Integer.parseInt(
            p.getProperty(OPERATION_ITERATIONS_PROPERTY, OPERATION_ITERATIONS_DEFAULT)
        );
        System.err.println("Repeating all call " + repetitionsPerOperation + " times");

        final String qI  = p.getProperty(QUERY_TIME_SPAN_PROPERTY, QUERY_TIME_SPAN_DEFAULT);
        if(qI == null) {
            System.err.println("No query time span found, but required!");
            System.exit(-1);
        }
        queryInterval = Duration.parse(qI);
        System.err.println("Using a call timespan of: " + queryInterval);
        operationchooser = createOperationGenerator(order);

        printQueryResults = Boolean.parseBoolean(
            p.getProperty(PRINT_QUERY_RESULTS_KEY, PRINT_QUERY_RESULTS_DEFAULT)
        );
        super.init(p);
    }

    @Override
    public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
        return super.initThread(p, mythreadid, threadcount);
    }

    @Override
    public final boolean doInsert(DB db, Object threadstate) {
        // currently we do not support inserting data for
        // this workload. this is pure read-only stuff
        return false;
    }

    @Override
    public final boolean doTransaction(DB db, Object threadstate) {
        SequenceItem<OperationType> item = operationchooser.nextValue();
        if(item == null || item.item == null) {
            return false;
        }
        if(! (db instanceof TelegrafDbIfc)) {
            throw new IllegalArgumentException("cannot handle non-TelegrafDBs " + db.getClass() + ". Make sure selected DB supports this kind of workload.");
        }
        final Vector<HashMap<String, ByteIterator>> cells =
            item.index < RESULTS_TRACKER_LENGTH
            ? results[item.index].cells
            : new Vector<HashMap<String, ByteIterator>>();
        if(item.index < RESULTS_TRACKER_LENGTH) {
            results[item.index].operation = item.item;
        }
        ((TelegrafDbIfc) db).tsQuery(item.item, queryInterval, cells);
        return true;
    }


  static interface OperationTypeInterface {

  }
    public static enum OperationType implements OperationTypeInterface {
        Q01("q01", 01),
        Q02("q02", 02),
        Q03("q03", 03),
        Q04("q04", 04),
        Q05("q05", 5),
        Q06("q06", 6),
        Q07("q07", 7),
        Q08("q08", 8),
        Q09("q09", 9),
        Q10("q10", 10),
        Q11("q11", 11),
        Q12("q12", 12),
        Q13("q13", 13),
        Q14("q14", 14),
        Q15("q15", 15),
        Q16("q16", 16),
        Q17("q17", 17),
        Q18("q18", 18),
        Q19("q19", 19),
        Q20("q20", 20),
        Q21("q21", 21),
        Q22("q22", 22),
        Q23("q23", 23),
        Q24("q24", 24),
        Q25("q25", 25),
        Q26("q26", 26),
        Q27("q27", 27);
        private final String id;
        private final int prio;
        OperationType(String id, int prio) {
            this.id = id;
            this.prio = prio;
        }
    }
    /**
     * A comma serparated list of supported operations. If not set, all 
     * operations are being used.
     */
    public static final String SUPPORTED_OPERATIONS_PROPERTY = "operations";

    /**
     * The default list of used operations (all available operations)
     */
    public static final String SUPPORTED_OPERATIONS_DEFAULT = "ALL";

    /**
     * The name of the property for the number of iterations per operation.
     */
    public static final String OPERATION_ITERATIONS_PROPERTY = "iterations";

    /**
     * The name of the property for the number of iterations per operation.
     */
    public static final String OPERATION_ORDER_PROPERTY = "order";
    /**
     * The name of the property for the number of iterations per operation.
     */
    public static final String OPERATION_ORDER_DEFAULT = "depth";
    /**
     * The name of the property for the flag whether to print query results
     */
    public static final String PRINT_QUERY_RESULTS_KEY = "printqueryresults";
    /**
     * The name of the property for the number of iterations per operation.
     */
    public static final String PRINT_QUERY_RESULTS_DEFAULT = "false";
    /**
     * Default number of iterations.
     */
    public static final String OPERATION_ITERATIONS_DEFAULT = "10";

    /**
     * The name of the property for the time span queries should look to back (interval)
     */
    public static final String QUERY_TIME_SPAN_PROPERTY = "timespan";

    /**
     * Default number of iterations.
     */
    public static final String QUERY_TIME_SPAN_DEFAULT = null;
}