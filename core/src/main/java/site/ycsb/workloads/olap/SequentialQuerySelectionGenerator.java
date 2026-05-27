package site.ycsb.workloads.olap;

import java.util.ArrayList;
import java.util.List;

import site.ycsb.generator.CounterGenerator;
import site.ycsb.generator.Generator;

public final class SequentialQuerySelectionGenerator extends Generator<String>{

    private final List<String> queryKeys;
    private final int repetitionsPerQuery;
    private final CounterGenerator generator = new CounterGenerator(0);

    SequentialQuerySelectionGenerator(List<String> queryKeys, int repetitionsPerQuery) {
        this.queryKeys = new ArrayList<>(queryKeys);
        this.repetitionsPerQuery = repetitionsPerQuery;
    }

    private String computeQuery(long value) {
        // assuming repetitionsPerQuery == 3
        // 0 / 3 => 0
        // 1 / 3 => 0
        // 2 / 3 => 0
        // 3 / 3 => 1
        long rawQueryNumber = value / repetitionsPerQuery;
        // assuming we have passed list length
        // queryKey.size() == 4
        // value == 13
        // 13 / 3 => 4
        //      4 % 4 = 0
        // 14 / 3 => 4
        //      4 % 4 = 0
        // 15 / 3 => 5
        //      5 % 4 = 1
        // cast ist safe as "% <number>" oscillates between [0; number - 1]
        int queryNumber = (int) (rawQueryNumber % queryKeys.size());
        return queryKeys.get(queryNumber);
    }

    public String nextValue() {
        long l = generator.nextValue();
        return computeQuery(l);
    }

    public String lastValue() {
        long l = generator.lastValue();
        return computeQuery(l);
    }
}
