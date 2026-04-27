package com.benchant.realtimeiot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.htrace.core.Tracer;

import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.workloads.CoreWorkload;

public final class RIotQueryWorkload extends Workload<RtIotDb> {

    public static final int MAX_NUM_QUERIES = 15;
    private static final int MAX_QUERY_DIGITS = Math.max(computeDigits(MAX_NUM_QUERIES), 2);
    /**
     * The query ids to use for this benchmark run.
     */
    public static final String TABLENAME_PROPERTY = "table";

    /**
     * The default name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

    /**
     * The default name of the database table to run queries against.
     */
    // public static final String ANALYTICS_QUERY_NUMBER_PROPERTY = "tpcds.queryid";
    // public static final String ANALYTICS_QUERY_NUMBER_PROPERTY_DEFAULT = "1";

    public static final String TPCDS_QUERY_SEARCH_PREFIX_PROPERTY = "tpcds.searchprefix";
    public static final String TPCDS_QUERY_SEARCH_PREFIX_PROPERTY_DEFAULT = "";

    public static final String TPCDS_QUERIES_LOWER_END = "tpcds.queries.lower";
    public static final String TPCDS_QUERIES_UPPER_END = "tpcds.queries.upper";
    public static final String TPCDS_QUERIES_LIST = "tpcds.queries.list";

    public static final String QUERY_PREFIX = "query";

    private QueryParameterGenerator parameterGenerator;

    @Override
    public RtIotDb createDbWrapper(RtIotDb db, Tracer tracer) {
        return new RtIotDBWrapper(db, tracer);
    }

    @Override
    public Class<RtIotDb> getDBClass() {
        return RtIotDb.class;
    }

    
    static class MySupplier implements Supplier<Integer> {
        @Override
        public Integer get() {
            return 1;
        }
    }

    private final ThreadLocal<Integer> iterationCounter = ThreadLocal.withInitial(new MySupplier());
    protected String table;
    protected DiscreteGenerator querychooser;
    protected String theQueryId = null;

    @Override
    public boolean doInsert(RtIotDb db, Object threadstate) {
        throw new IllegalStateException("doInsert is not supported for AnalyticsQueryWorkload");
    }

    private static int computeDigits(int a) {
        int digits = 0;
        int tmp = Math.abs(a);
        while(tmp > 9) {
            digits = digits + 1;
            tmp = tmp / 10;
        }
        return digits + 1;
    }

    public static String getPaddedQueryId(int value, int maxDigits) {
        final int myDigits = computeDigits(value);
        StringBuilder b = new StringBuilder();
        for(int j = myDigits; j < maxDigits; j++) {
            b.append("0");
        }
        b.append(value);
        return QUERY_PREFIX + b.toString();
    }

    private void initQueries(Properties p) throws IOException {
        String searchPrefix = p.getProperty(TPCDS_QUERY_SEARCH_PREFIX_PROPERTY);
        for(int i = 1; i <= MAX_NUM_QUERIES; i++) {
            String key = getPaddedQueryId(i, MAX_QUERY_DIGITS);
            String path = searchPrefix == null
                ? key
                : searchPrefix + "/" + key;
            System.err.println("searching for resource " + path);
            InputStream s = this.getClass().getClassLoader().getResourceAsStream(path);
            if(s == null) {
                // System.err.println("resource " + key + " was not found");
                continue;
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(s));
            StringBuilder b = new StringBuilder();
            while(true) {
                String line = br.readLine();
                if(line == null) {
                    break;
                }
                b.append(line);
            }
            System.err.println("adding query: " + key + " --- " + b.toString());
            QueryRegistry.INSTANCE.addQuery(key, b.toString());
        }
    }

    private static DiscreteGenerator initOperationChooser(int lowerQueryEnd, int upperQueryEnd, List<String> qList) throws WorkloadException {
        int options = 0;
        DiscreteGenerator querychooser = new DiscreteGenerator();
        for(int i = lowerQueryEnd; i <= upperQueryEnd; i++) {
            String key = getPaddedQueryId(i, MAX_QUERY_DIGITS);
            if(qList == null || qList.contains(key)) {
                System.err.println("Adding query as option: " + key);
                options++;
                querychooser.addValue(1, key);
            }
        }
        if(options == 0) {
            throw new WorkloadException("no query adeed to querychooser");
        }
        return querychooser;
    }

    private List<String> initQueryList(String queryList) {
        if(queryList == null) return null;
        List<String> queries = new ArrayList<>();
        String[] elements = queryList.split(",");
        for(String s : elements) {
            String t = s.trim();
            try { 
                int j = Integer.parseInt(t); 
                t = getPaddedQueryId(j, MAX_QUERY_DIGITS);
            } catch(NumberFormatException nex) {}
            queries.add(t);
        }
        return queries;
    }

    @Override
    public void init(Properties p) throws WorkloadException {
        System.err.println("Initializing TPC-DS Workload");
        table = p.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
        long threadcount = Integer.parseInt(p.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
        long opcount = Long.parseLong(p.getProperty(Client.OPERATION_COUNT_PROPERTY, "0"));
        if(threadcount > opcount) {
            System.err.println("more threads than operations. this is not useful. aborting.");
            System.exit(-1);
        } else if((opcount % threadcount) != 0) {
            System.err.println("WARNING: operationcount is not a multiple of threadcount. may lead to unequal repetitions per thread.");
        }
        System.err.println("running " + (opcount / threadcount) + " iterations per thread");
        /* intialize queries */
        try {
            initQueries(p);
        } catch(IOException e) {
            throw new WorkloadException(e);
        }
        int lowerQueryEnd = Integer.parseInt(p.getProperty(TPCDS_QUERIES_LOWER_END, "1"));
        int upperQueryEnd = Integer.parseInt(p.getProperty(TPCDS_QUERIES_UPPER_END, Integer.toString(MAX_NUM_QUERIES)));
        String queryArray = p.getProperty(TPCDS_QUERIES_LIST);
        upperQueryEnd = Math.min(upperQueryEnd, MAX_NUM_QUERIES);
        /*
        int queryid = Integer.parseInt(p.getProperty(ANALYTICS_QUERY_NUMBER_PROPERTY, ANALYTICS_QUERY_NUMBER_PROPERTY_DEFAULT));
        if(queryid < 1 || queryid > MAX_NUM_QUERIES) {
            System.err.println("queryid '" + queryid + "' is invalid. Max ID is " + MAX_NUM_QUERIES);
        } else {
            System.err.println("queryid: " + queryid);
        }
        theQueryId = getPaddedQueryId(queryid, MAX_QUERY_DIGITS);
        String query = QueryRegistry.INSTANCE.getQuery(theQueryId);
        if(query == null) {
            System.err.println("query for queryid " + theQueryId + " not found. Aborting.");
            System.exit(-1);
        } else {
            System.err.println("query for queryid: " + query);
        }
        p.setProperty(ANALYTICS_QUERY_NUMBER_PROPERTY, theQueryId);
        */
        // initilize order in which queries are run
        // as of now, we only support randomly picking
        // a query
        List<String> queryList = initQueryList(queryArray);
        if(queryList != null && queryList.isEmpty()) {
            throw new WorkloadException("cannot deal with an empty query list");
        }
        parameterGenerator = new QueryParameterGenerator(p);
        querychooser = initOperationChooser(lowerQueryEnd, upperQueryEnd, queryList);
    }

    private String pickQuery() {
        return querychooser.nextString();
        // return theQueryId;
    }

    @Override
    public boolean doTransaction(RtIotDb db, Object threadstate) {
        Integer iteration = iterationCounter.get();
        String query = pickQuery();
        List<HashMap<String, ByteIterator>> result = new ArrayList<>();
        Map<String, Object> params = parameterGenerator.generateParameters(query);
        db.query(query, params, result);
        iterationCounter.set(iteration + 1);
        return true;
    }
}
