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
package site.ycsb.db;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.query.QueryOptions;
import site.ycsb.ByteIterator;
import site.ycsb.TelegrafDB;
import site.ycsb.workloads.TelegrafWorkload.OperationType;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * InfluxDB binding for YCSB framework using the InluxDB v3 Client <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 */
public class InfluxDbClient extends TelegrafDB  {
  private static final Object INCLUDE = new Object();
  private final static Map<OperationType, String> OPERATION_MAP;
  static {
      OPERATION_MAP = new HashMap<>();
      // OPERATION_MAP.put(OperationType.Q01, "select count(*) from benchant.cpu where time > now() - interval '%s'");
      // OPERATION_MAP.put(OperationType.Q04, "select * from procstat where pid =~ /44/ and time > now() - interval '%s' order by time desc");
      // OPERATION_MAP.put(OperationType.Q09, "select spread(cpu_time_guest_nice), stddev(cpu_time_guest_nice) from procstat where time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q01, "select count(*) from procstat where time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q02, "select count(write_count), count(process_name), count(ppid), count(rlimit_memory_vms_hard) from procstat where time > now() - interval '%s' and write_bytes " + Q02_WRITE_BYTES_FILTER);
      OPERATION_MAP.put(OperationType.Q03, "SELECT * FROM procstat where cpu_time " + Q03_CPU_TIME_FILTER + " and time > (now() - interval '%s') order by time desc limit 5");
      OPERATION_MAP.put(OperationType.Q04, "SELECT count(*) FROM procstat where pid like '%%44%%' and time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q05, "select * from procstat where time > now() - interval '%s' limit 10");
      OPERATION_MAP.put(OperationType.Q06, "select distinct(memory_data) from procstat where cpu_time " + Q06_CPU_TIME_FILTER + " and time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q07, "SELECT count(read_bytes), count(read_count), instance FROM procstat where num_threads "+ Q07_THREADS_FILTER + " and time > now() - interval '%s' Group by instance");
      OPERATION_MAP.put(OperationType.Q08, "select sum(rlimit_num_fds_soft), sum(rlimit_realtime_priority_hard), sum(rlimit_realtime_priority_soft), sum(rlimit_signals_pending_hard) from procstat where time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q09, "SELECT sqrt(avg(cpu_time)), sin(avg(cpu_time)), cos(avg(cpu_time)), pow(avg(cpu_time),56456), round(avg(cpu_time)) FROM procstat where time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q10, "SELECT avg(minor_faults), memory_usage FROM procstat where cpu_time " + Q10_CPU_TIME_FILTER + " and time > now() - interval '%s' Group by memory_usage having memory_usage " + Q10_MEMORY_FILTER);
      OPERATION_MAP.put(OperationType.Q11, "SELECT min(memory_usage) FROM procstat where cpu_time " + Q11_CPU_TIME_FILTER + " and time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q12, "select max(cpu_time) from procstat where memory_usage " + Q12_MEMORY_USAGE_FILTER + " and time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q13, "select min(memory_usage) from procstat where cpu_time " + Q13_CPU_TIME_FILTER + " and time > now() - interval '%1$s' Union ALL select max(cpu_time) from procstat where memory_usage " + Q13_MEMORY_USAGE_FILTER + " and time > now() - interval '%1$s'");
      OPERATION_MAP.put(OperationType.Q14, "select count(cpu_time) + sum(cpu_time) from procstat where time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q15, "select sum(write_count) from procstat where write_bytes " + Q15_WRITE_BYTES_FILTER + " and instance in ('instance-0-0', 'instance-0-1', 'instance-0-2') and time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q16, "select count(write_count) from procstat where write_count is not null and (instance ='instance-1-5' or instance ='instance-1-6') and time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q17, "SELECT all(child_major_faults) FROM procstat where instance ='instance-0-8' and time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q18, "show tables");
      OPERATION_MAP.put(OperationType.Q19, "select date_trunc('second', time) as time_bucket, max(time), count(*) from procstat where time > now() - interval '%s' group by time_bucket order by time_bucket");
      OPERATION_MAP.put(OperationType.Q20, "SELECT median(memory_usage) FROM procstat where (instance ='instance-0-1' or instance ='instance-0-2') and time > now() - interval '%s'");
      OPERATION_MAP.put(OperationType.Q21, "select date_bin(interval '15 minute', time, timestamp '1970-01-01T00:00:00Z') as binned, mean(\"cpu_time\") as value, approx_percentile_cont(\"cpu_time\", 0.99) as perc, coalesce(instance, 'instance-74') as instance from procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' and time > now() - interval '%s' group by instance,binned");
      OPERATION_MAP.put(OperationType.Q22, "select binned as time, value/1000000000 as mean, perc as p99, 'duration' as _field, instance from ( select date_bin(interval '15 minute', time, timestamp '1970-01-01T00:00:00Z') as binned, mean(\"cpu_time\") as value, approx_percentile_cont(\"cpu_time\", 0.99) as perc, coalesce(instance, 'instance-74') as instance from procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' and time > now() - interval '%s' group by instance,binned) ORDER BY time ASC");
      OPERATION_MAP.put(OperationType.Q23, "SELECT binned as time, mean, p99 FROM ( select date_bin(interval '15 minute', time, timestamp '1970-01-01T00:00:00Z') as binned, mean(\"cpu_time\") as mean, approx_percentile_cont(\"cpu_time\", 0.99) as p99 from procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' AND time > now() - interval '%s' GROUP BY binned) ORDER BY binned ASC");
      OPERATION_MAP.put(OperationType.Q24, "SELECT \"cpu_time_system\", \"cpu_time\", \"process_name\", \"cpu_time_user\" FROM procstat WHERE \"instance\" = 'instance-0-11' AND time >= now() - interval  '%s' ORDER BY \"process_name\" DESC");
      OPERATION_MAP.put(OperationType.Q25, "select binned as time, value as _value, \"process_name\", 'cpu_time' as _field from (select date_bin(interval '%1$s', time, timestamp '1970-01-01T00:00:00Z') as binned, count(\"instance\") as value, \"process_name\" from procstat where \"process_name\" like '%%kworker%%' and time > now() - interval '%1$s' group by \"process_name\", binned) order by time");   
      OPERATION_MAP.put(OperationType.Q26, "select binned as time, value as _value, process_name, CASE process_name WHEN 'telegraf' THEN '01-telegraf' WHEN 'dockerd' THEN '02-dockerd' WHEN 'containerd' THEN '03-containerd' WHEN 'bash' THEN '04 - bash' WHEN 'systemd-logind' THEN '05 - systemd-logind' WHEN 'pgrep' THEN '06 - pgrep' WHEN 'cron' THEN '07 - cron' WHEN 'snapd' THEN '08 - snapd' else process_name END as _field from (select date_bin(interval '%1$s', time, timestamp '1970-01-01T00:00:00Z') as binned, count(\"cpu_time\") as value, \"process_name\" from procstat where \"host\" like '%%benchmark%%' and process_name != 'systemd-networkd' and process_name != 'systemd' and time > (now() - interval '%1$s') group by \"process_name\", binned) order by time");
      OPERATION_MAP.put(OperationType.Q27, "SELECT sum(\"_value\") as \"_value\" FROM (SELECT sum(\"_value\") as \"_value\", \"instance\" FROM (SELECT max(cpu_time) as \"_value\", instance, process_name FROM procstat where \"process_name\" like '%%kworker%%' AND time > (now() - interval '%s') GROUP BY \"instance\", \"process_name\" ) GROUP BY \"instance\")");
  }
  private static InfluxDBClient influxDbClient;

  // override queries by configuration
  private void initQueries() {
    System.out.println("[Startup] initializing operations for InfluxDbClient");
    for(OperationType op : OperationType.values()) {
      String rawQuery = OPERATION_MAP.get(op);
      String extQuery = getProperties().getProperty("influxdb.queries." + op.toString().toLowerCase(), rawQuery);
      OPERATION_MAP.put(op, extQuery);
      System.out.println("[Startup] " + op + ": " + extQuery);
    }
  }

  @Override
  public void init() {
    super.init();
    synchronized (INCLUDE) {
      if (influxDbClient != null) {
        return;
      }
      Properties props = getProperties();
      String host = props.getProperty("influxdb.url", "");
      char[] token = System.getenv("INFLUX_ENTERPRISE_TOKEN").toCharArray();
      String database = props.getProperty("influxdb.database", "");
      initQueries();
      ClientConfig clientConfig = new ClientConfig.Builder().token(token).database(database).host(host)
          .organization("baas").build();
      try {
        debug("initializing client");
        influxDbClient = InfluxDBClient.getInstance(clientConfig);
        debug("initializion complete");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }
  }

  private String durationToTimestreamString(Duration timeInterval) {
    return timeInterval.toMinutes() + " minute";
  }

  @Override
  public final Status tsQuery(OperationType op, Duration timeInterval, Vector<HashMap<String, ByteIterator>> result) {
    String rawQuery = OPERATION_MAP.get(op);
    if(rawQuery == null) {
      System.err.println("query " + op + " not found");
      return Status.NOT_FOUND;
    }
    String sql = String.format(rawQuery, durationToTimestreamString(timeInterval));
    if(doDebug) {
      debug("\n\nquerying(" + op + "): " + sql);
    }
    /*
    try (Stream<VectorSchemaRoot> root = influxDbClient.queryBatches(rawQuery)){
      List<VectorSchemaRoot> results = root.collect(Collectors.toList());
      results.forEach(vector -> System.err.println("\tresults: " + vector.getRowCount()));
     }
     */
    try (Stream<Object[]> stream = influxDbClient.query(sql, QueryOptions.DEFAULTS)) {
      List<Object[]> results = stream.collect(Collectors.toList());
      // System.err.println("result size: " + results.size());
      results.forEach(row -> {
        HashMap<String, ByteIterator> parsed = new HashMap<>();
        // debug("\tresults: " + Arrays.toString(row))
        for(int i = 0; i < row.length; i++) {
          Object data = row[i];
          if(null == data) {
            parsed.put(Integer.toString(i), new StringByteIterator("<nil>"));
          } else {
            parsed.put(Integer.toString(i), new StringByteIterator(data.toString()));  
          }
        }
        result.add(parsed);
      });
      if(doDebug) {
        debug("\n\tresults");
        result.forEach(row -> debug("\t\t data: " + row.toString()));
      }
      // System.err.println("query ended: " + stream);
      // stream.forEach(row -> System.err.println("\tresults: " + Arrays.toString(row)));
    }
    return Status.OK;
  }
}