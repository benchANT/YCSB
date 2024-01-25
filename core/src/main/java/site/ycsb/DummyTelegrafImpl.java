/*
 * Copyright (c) 2023 - 2024, benchANT GmbH. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import site.ycsb.workloads.TelegrafWorkload.OperationType;

public final class DummyTelegrafImpl extends TelegrafDB {

    private static Random random = new Random();
    private final static Map<OperationType, String> OPERATION_MAP;
    static {
        OPERATION_MAP = new HashMap<>();
        OPERATION_MAP.put(OperationType.Q01, "select count(*) from procstat where time > now() - <interval>");
        OPERATION_MAP.put(OperationType.Q02, "select count(write_count), count(process_name), count(ppid), count(rlimit_memory_vms_hard) from procstat where time > now() - >interval> and write_bytes > 20000");
        OPERATION_MAP.put(OperationType.Q04, "select * from procstat where pid =~ /44/ and time > now() - <interval> order by time desc");
        OPERATION_MAP.put(OperationType.Q05, "select * from procstat where time > now() - <interval> limit 10;");
        OPERATION_MAP.put(OperationType.Q06, "select distinct(memory_data) from procstat where cpu_time > 90 and time > now() - <interval>");
        OPERATION_MAP.put(OperationType.Q08, "select sum(rlimit_num_fds_soft), sum(rlimit_realtime_priority_hard), sum(rlimit_realtime_priority_soft), sum(rlimit_signals_pending_hard) from procstat where time > now() - <interval>;");
        OPERATION_MAP.put(OperationType.Q09, "select spread(cpu_time_guest_nice), stddev(cpu_time_guest_nice) from procstat where time > now() - <interval>;");
        OPERATION_MAP.put(OperationType.Q12, "select max(cpu_time) from procstat where memory_usage > 0.1 and time > now() - <interval>");
        OPERATION_MAP.put(OperationType.Q13, "select min(memory_usage) from procstat where cpu_time > 80 and time > now() - <interval>; select max(cpu_time) from procstat where memory_usage > 0.1 and time > now() - <interval>;");
        OPERATION_MAP.put(OperationType.Q14, "select count(cpu_time) + count(cpu_time_idle) from procstat where num_threads > 20 and time > now() - <interval> group by instance");
        OPERATION_MAP.put(OperationType.Q15, "select sum(write_count) from procstat where write_bytes > 2000000 and instance =~ /instance-00|instance-01|instance-02/ and time > now() - <interval>;");
        OPERATION_MAP.put(OperationType.Q16, "select count(write_count) from procstat where (instance ='instance-05' or instance ='instance-06') and time > now() - <interval>");
        OPERATION_MAP.put(OperationType.Q19, "select count(ppid) from procstat where time > now() - <interval> group by time(1s)");
        OPERATION_MAP.put(OperationType.Q20, "select median(write_count) from procstat where (instance ='instance-01' or instance ='instance-02') and time > now() - <interval>;");
        OPERATION_MAP.put(OperationType.Q21, "select top(\"cpu_time\", 5) from procstat where time > now() - <interval>;");
        OPERATION_MAP.put(OperationType.Q23, "select difference(mean(cpu_time)) from procstat where time > now() - <interval> group by time(1s)");
        OPERATION_MAP.put(OperationType.Q24, "select derivative(\"cpu_time_idle\",1s) from procstat where time > now() - <interval>;");
        OPERATION_MAP.put(OperationType.Q27, "select non_negative_difference(cpu_time) from procstat where time > now() - <interval> ;");
    }

    public DummyTelegrafImpl() {
        System.err.println("intializing dummy telegraf db impl");
    }

 @Override
 public Status tsQuery(OperationType ot, Duration timeInterval, Vector<HashMap<String, ByteIterator>> result) {
    // select count(ppid) from procstat where time > now() - <interval>; 
    final String query = OPERATION_MAP.get(ot);
    System.err.println("query: " + ot + " " + query);
    int sleep = (random.nextInt(4) + 8) * 100;
    try {
        Thread.sleep(sleep);
    } catch (InterruptedException ie) {
        // does not matter
    }
    return Status.OK;
 }
    /*
        select sum(rlimit_num_fds_soft), sum(rlimit_realtime_priority_hard),
        sum(rlimit_realtime_priority_soft), sum(rlimit_signals_pending_hard)
        from procstat where time > now() - <interval>;
    /*
     q3-orderbydesc|1|5
        Group by / Order by
        select * from procstat where cpu_time > 45 and time > now() - <interval> order by time desc;
    q12-max|1|5
        Threshold
        select max(cpu_time) from procstat where memory_usage > 0.1 and time > now() - <interval>;
q14-operators|1|5
    Group by / Order by
    select count(cpu_time) + count(cpu_time_idle) from procstat where num_threads > 20 and time > now() - <interval> group by instance;
q19-windowing_max_second|1|5
    Group by time
    select count(ppid) from procstat where time > now() - <interval> group by time(1s);
q23-differencemean|1|5
    Group by time
    select difference(mean(cpu_time)) from procstat where time > now() - <interval> group by time(1s);
q24-derivative|1|5
    select derivative("cpu_time_idle",1s) from procstat where time > now() - <interval>;
Group by time
q15-in|1|5
    In / Or
    select sum(write_count) from procstat where write_bytes > 2000000 and instance =~ /instance-00|instance-01|instance-02/ and time > now() - <interval>;
q16-or|1|5
    In / Or
    select count(write_count) from procstat where (instance ='instance-05' or instance ='instance-06') and time > now() - <interval>;
q4-like|1|5
    Like / Union
    select * from procstat where pid =~ /44/ and time > now() - <interval> order by time desc;
q13-union|1|5
    Like / Union
    select min(memory_usage) from procstat where cpu_time > 80 and time > now() - <interval>;
    select max(cpu_time) from procstat where memory_usage > 0.1 and time > now() - <interval>;
q5-limit|1|5
    Limit / Top / Bottom
    select * from procstat where time > now() - <interval> limit 10;
q21-top|1|5
    Limit / Top / Bottom
    select top("cpu_time", 5) from procstat where time > now() - <interval>;
q9-mathematical|1|5
    Statistic / Math
    select spread(cpu_time_guest_nice), stddev(cpu_time_guest_nice) from procstat where time > now() - <interval>;
q20-median|2|5
    Statistic / Math
    select median(write_count) from procstat where (instance ='instance-01' or instance ='instance-02') and time > now() - <interval>;
q27-nonnegativedifference|1|5
    Statistic / Math
    select non_negative_difference(cpu_time) from procstat where time > now() - <interval> ;
q2-field_count|1|5
    Threshold
    select count(write_count), count(process_name), count(ppid), count(rlimit_memory_vms_hard) from procstat where time > now() -
<interval> and write_bytes > 20000;
q6-distinct|1|5
    Threshold
    select distinct(memory_data) from procstat where cpu_time > 90 and time > now() - <interval>;
*/
    private String getTail(String table, String interval) {
    return " from " + table + " where time > now() - " + interval + ";";
    }
    private String getTimeSnippet(String interval) {
        return " where time > now() - " + interval;
    }
    private String getOrderSnippet(String orderDirection, String orderColumn) {
        return "order by " + orderColumn + " " + orderDirection; 
    }
    private String getFilterSnippet(String filterColumn, String filter) {
        return "where " + filterColumn + " " + filter; 
    }
}