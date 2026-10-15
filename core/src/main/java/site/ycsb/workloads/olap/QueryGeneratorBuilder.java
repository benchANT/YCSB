package site.ycsb.workloads.olap;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import site.ycsb.WorkloadException;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.generator.Generator;

public interface QueryGeneratorBuilder {
    public static final int MAX_NUM_QUERIES = 32;
    public static final int MAX_QUERY_DIGITS = Math.max(QueryGeneratorBuilder.computeDigits(MAX_NUM_QUERIES), 2);

    public static final String QUERYS_SELECTION_STRATEGY_PROPERTY = "olap.queryselection";
    public static final String QUERYS_SELECTION_STRATEGY_PROPERTY_DEFAULT = "random";
    public static final String QUERYS_SEQUENTIAL_SELECTION_STRATEGY_COUNT_PROPERTY = "olap.queryselection.sequential.count";
    public static final String QUERYS_SEQUENTIAL_SELECTION_STRATEGY_COUNT_PROPERTY_DEFAULT = "1";
    
    public static final String QUERY_PREFIX = "query";

    public static QueryGeneratorBuilder getBuilder(Properties prop) throws WorkloadException {
        String selectionStrategy = prop.getProperty(QUERYS_SELECTION_STRATEGY_PROPERTY, QUERYS_SELECTION_STRATEGY_PROPERTY_DEFAULT);
        switch (selectionStrategy) {
            case "random":
                return new DiscreteGeneratorBuilder(prop);
            case "n-sequential":
                return new CoutingGeneratorBuilder(prop);
            default:
                throw new WorkloadException("Unknown query selection strategy: " + selectionStrategy);
        }
    }

    public static Generator<?> initOperationChooser(Properties p, int lowerQueryEnd, int upperQueryEnd, List<String> qList) throws WorkloadException {
        int options = 0;
        QueryGeneratorBuilder builder = getBuilder(p);
        for(int i = lowerQueryEnd; i <= upperQueryEnd; i++) {
            String key = getPaddedQueryId(i, MAX_QUERY_DIGITS);
            if(qList == null || qList.contains(key)) {
                System.err.println("Adding query as option: " + key);
                options++;
                builder.addQuery(key);
            }
        }
        if(options == 0) {
            throw new WorkloadException("no query adeed to querychooser");
        }
        return builder.build();
    }

    public static int computeDigits(int a) {
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

    public void addQuery(String query);
    public Generator<?> build();

    public static class CoutingGeneratorBuilder implements QueryGeneratorBuilder {
        private final List<String> queryKeys = new ArrayList<>();
        private final int repetitionsPerQuery;
        CoutingGeneratorBuilder(Properties props) {
            repetitionsPerQuery = Integer.parseInt(props.getProperty(QUERYS_SEQUENTIAL_SELECTION_STRATEGY_COUNT_PROPERTY, QUERYS_SEQUENTIAL_SELECTION_STRATEGY_COUNT_PROPERTY_DEFAULT));
        }

        public void addQuery(String key) {
            queryKeys.add(key);
        }

        public Generator<?> build() {
            return new SequentialQuerySelectionGenerator(queryKeys, repetitionsPerQuery);
        }
    }

    public static class DiscreteGeneratorBuilder implements QueryGeneratorBuilder {

        private final DiscreteGenerator querychooser = new DiscreteGenerator();

        DiscreteGeneratorBuilder(Properties props) {
            // nothing needed
        }

        public void addQuery(String key) {
            querychooser.addValue(1, key);
        }

        public Generator<?> build() {
            return querychooser;
        }
    }
}
