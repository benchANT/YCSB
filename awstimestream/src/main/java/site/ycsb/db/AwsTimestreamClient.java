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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.TelegrafDB;
import site.ycsb.workloads.TelegrafWorkload.OperationType;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.ColumnInfo;
import software.amazon.awssdk.services.timestreamquery.model.Datum;
import software.amazon.awssdk.services.timestreamquery.model.QueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.QueryStatus;
import software.amazon.awssdk.services.timestreamquery.model.Row;
import software.amazon.awssdk.services.timestreamquery.model.TimeSeriesDataPoint;
import software.amazon.awssdk.services.timestreamquery.model.Type;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.CreateTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties;
import software.amazon.awssdk.services.timestreamwrite.model.TimestreamWriteException;

/**
 * AWS Timestream binding for YCSB framework using the Java v2 AWS Timestream <a
 * href="https://docs.aws.amazon.com/timestream/latest/developerguide/getting-started.java-v2.html">Client</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 */
public class AwsTimestreamClient extends TelegrafDB  {
  private static final long ONE_GB_IN_BYTES = 1073741824L;
  private static final Object INCLUDE = new Object();
  private final static Map<OperationType, String> OPERATION_MAP;
  static {
      OPERATION_MAP = new HashMap<>();
      OPERATION_MAP.put(OperationType.Q01, "select count(*) from \"%1$s\".procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q02, "select count(write_count), count(process_name), count(ppid), count(rlimit_memory_vms_hard) from \"%1$s\".procstat where time > now() - %2$s and write_bytes " + Q02_WRITE_BYTES_FILTER);
      OPERATION_MAP.put(OperationType.Q03, "SELECT * FROM \"%1$s\".procstat where cpu_time " + Q03_CPU_TIME_FILTER + " and time > (now() - %2$s) order by time desc limit 5");
      OPERATION_MAP.put(OperationType.Q04, "SELECT count(*) FROM \"%1$s\".procstat where pid like '%%44%%' and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q05, "select * from \"%1$s\".procstat where time > now() - %2$s limit 10");
      OPERATION_MAP.put(OperationType.Q06, "select distinct(memory_data) from \"%1$s\".procstat where cpu_time " + Q06_CPU_TIME_FILTER + " and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q07, "SELECT count(read_bytes), count(read_count), instance FROM \"%1$s\".procstat where num_threads "+ Q07_THREADS_FILTER + " and time > now() - %2$s Group by instance");
      OPERATION_MAP.put(OperationType.Q08, "select sum(rlimit_num_fds_soft), sum(rlimit_realtime_priority_hard), sum(rlimit_realtime_priority_soft), sum(rlimit_signals_pending_hard) from \"%1$s\".procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q09, "SELECT sqrt(avg(cpu_time)), sin(avg(cpu_time)), cos(avg(cpu_time)), pow(avg(cpu_time),56456), round(avg(cpu_time)) FROM \"%1$s\".procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q10, "SELECT avg(minor_faults), memory_usage FROM \"%1$s\".procstat where cpu_time " + Q10_CPU_TIME_FILTER + " and time > now() - %2$s Group by memory_usage having memory_usage " + Q10_MEMORY_FILTER);
      OPERATION_MAP.put(OperationType.Q11, "SELECT min(memory_usage) FROM \"%1$s\".procstat where cpu_time " + Q11_CPU_TIME_FILTER + " and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q12, "select max(cpu_time) from \"%1$s\".procstat where memory_usage " + Q12_MEMORY_USAGE_FILTER + " and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q13, "select min(memory_usage) from \"%1$s\".procstat where cpu_time " + Q13_CPU_TIME_FILTER + " and time > now() - %2$s Union ALL select max(cpu_time) from \"%1$s\".procstat where memory_usage " + Q13_MEMORY_USAGE_FILTER + " and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q14, "select count(cpu_time) + sum(cpu_time) from \"%1$s\".procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q15, "select sum(write_count) from \"%1$s\".procstat where write_bytes " + Q15_WRITE_BYTES_FILTER + " and instance in ('instance-0-0', 'instance-0-1', 'instance-0-2') and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q16, "select count(write_count) from \"%1$s\".procstat where write_count is not null and (instance ='instance-1-5' or instance ='instance-1-6') and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q17, "SELECT all(child_major_faults) FROM \"%1$s\".procstat where instance ='instance-0-8' and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q18, "SHOW TABLES FROM %1$s");
      OPERATION_MAP.put(OperationType.Q19, "select date_trunc('second', time) as time_bucket, max(time), count(*) from \"%1$s\".procstat where time > now() - %2$s group by 1 order by 1");
      OPERATION_MAP.put(OperationType.Q20, "SELECT approx_percentile(memory_usage, 0.5) FROM \"%1$s\".procstat where (instance ='instance-0-1' or instance ='instance-0-2') and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q21, "select bin(time, 15m) as binned, avg(\"cpu_time\") as value, approx_percentile(\"cpu_time\", 0.99) as perc, coalesce(instance, 'instance-74') as instance from \"%1$s\".procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' and time > now() - %2$s group by instance, 1");
      OPERATION_MAP.put(OperationType.Q22, "select binned as time, value/1000000000 as mean, perc as p99, 'duration' as _field, instance from ( select bin(time, 15m) as binned, avg(\"cpu_time\") as value, approx_percentile(\"cpu_time\", 0.99) as perc, coalesce(instance, 'instance-74') as instance from \"%1$s\".procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' and time > now() - %2$s group by instance,1) ORDER BY time ASC");
      OPERATION_MAP.put(OperationType.Q23, "SELECT binned as time, mean, p99 FROM ( select bin(time, 15m) as binned, avg(\"cpu_time\") as mean, approx_percentile(\"cpu_time\", 0.99) as p99 from \"%1$s\".procstat WHERE \"process_name\" like '%%kworker%%' and instance != 'instance-75' AND time > now() - %2$s GROUP BY 1) ORDER BY binned ASC");
      OPERATION_MAP.put(OperationType.Q24, "SELECT \"cpu_time_system\", \"cpu_time\", \"process_name\", \"cpu_time_user\" FROM \"%1$s\".procstat WHERE \"instance\" = 'instance-0-11' AND time >= now() - %2$s ORDER BY \"process_name\" DESC");
      OPERATION_MAP.put(OperationType.Q25, "select binned as time, value as _value, \"process_name\", 'cpu_time' as _field from (select bin(time, %2$s) as binned, count(\"instance\") as value, \"process_name\" from \"%1$s\".procstat where \"process_name\" like '%%kworker%%' and time > now() - %2$s group by \"process_name\", 1) order by time");
      OPERATION_MAP.put(OperationType.Q27, "SELECT sum(\"_value\") as \"_value\" FROM (SELECT sum(\"_value\") as \"_value\", \"instance\" FROM (SELECT max(cpu_time) as \"_value\", instance, process_name FROM \"%1$s\".procstat where \"process_name\" like '%%kworker%%' AND time > (now() - %2$s) GROUP BY \"instance\", \"process_name\" ) GROUP BY \"instance\")");     
      OPERATION_MAP.put(OperationType.Q26, "select binned as time, value as _value, process_name, CASE process_name WHEN 'telegraf' THEN '01-telegraf' WHEN 'dockerd' THEN '02-dockerd' WHEN 'containerd' THEN '03-containerd' WHEN 'bash' THEN '04 - bash' WHEN 'systemd-logind' THEN '05 - systemd-logind' WHEN 'pgrep' THEN '06 - pgrep' WHEN 'cron' THEN '07 - cron' WHEN 'snapd' THEN '08 - snapd' else process_name END as _field from (select bin(time, %2$s) as binned, count(\"cpu_time\") as value, \"process_name\" from \"%1$s\".procstat where \"host\" like '%%benchmark%%' and process_name != 'systemd-networkd' and process_name != 'systemd' and time > (now() - %2$s) group by \"process_name\", 1) order by time");      
      // OPERATION_MAP.put(OperationType.Q21, "select date_bin('15 minute', time, timestamp '1970-01-01T00:00:00Z') as binned, mean(\"cpu_time\") as value, approx_percentile_cont(\"cpu_time\", 0.99) as perc, coalesce(instance, 'instance-74') as instance from \"%1$s\".procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' and time > now() - %2$s group by instance,binned");
      // OPERATION_MAP.put(OperationType.Q23, "SELECT binned as time, mean, p99 FROM ( select bin(time, 15m) as binned, avg(\"cpu_time\") as mean, approx_percentile(\"cpu_time\", 0.99) as p99 from \"%1$s\".procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' AND time > now() - %2$s GROUP BY 1) ORDER BY binned ASC");
      /*
      OPERATION_MAP.put(OperationType.Q01, "select count(*) from procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q01, "select count(*) from \"%s\".cpu where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q02, "select count(write_count), count(process_name), count(ppid), count(rlimit_memory_vms_hard) from \"%s\".procstat where time > now() - %2$s and write_bytes > 20000");
      OPERATION_MAP.put(OperationType.Q04, "select * from \"%s\".procstat where pid =~ /44/ and time > now() - %2$s order by time desc");
      OPERATION_MAP.put(OperationType.Q05, "select * from \"%s\".procstat where time > now() - %2$s limit 10");
      OPERATION_MAP.put(OperationType.Q06, "select distinct(memory_data) from \"%s\".procstat where cpu_time > 90 and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q08, "select sum(rlimit_num_fds_soft), sum(rlimit_realtime_priority_hard), sum(rlimit_realtime_priority_soft), sum(rlimit_signals_pending_hard) from \"%s\".procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q09, "select spread(cpu_time_guest_nice), stddev(cpu_time_guest_nice) from \"%s\".procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q12, "select max(cpu_time) from \"%s\".procstat where memory_usage > 0.1 and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q13, "select min(memory_usage) from \"%s\".procstat where cpu_time > 80 and time > now() - %2$s; select max(cpu_time) from \"%s\".procstat where memory_usage > 0.1 and time > now() - %2$s;");
      OPERATION_MAP.put(OperationType.Q14, "select count(cpu_time) + count(cpu_time_idle) from \"%s\".procstat where num_threads > 20 and time > now() - %2$s group by instance");
      OPERATION_MAP.put(OperationType.Q15, "select sum(write_count) from \"%s\".procstat where write_bytes > 2000000 and instance =~ /instance-00|instance-01|instance-02/ and time > now() - %2$s;");
      OPERATION_MAP.put(OperationType.Q16, "select count(write_count) from \"%s\".procstat where (instance ='instance-05' or instance ='instance-06') and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q19, "select count(ppid) from \"%s\".procstat where time > now() - %2$s group by time(1s)");
      OPERATION_MAP.put(OperationType.Q20, "select median(write_count) from \"%s\".procstat where (instance ='instance-01' or instance ='instance-02') and time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q21, "select top(\"cpu_time\", 5) from \"%s\".procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q23, "select difference(mean(cpu_time)) from \"%s\".procstat where time > now() - %2$s group by time(1s)");
      OPERATION_MAP.put(OperationType.Q24, "select derivative(\"cpu_time_idle\",1s) from \"%s\".procstat where time > now() - %2$s");
      OPERATION_MAP.put(OperationType.Q27, "select non_negative_difference(cpu_time) from \"%s\".procstat where time > now() - %2$s");
      */
  }

  private static TimestreamQueryClient timestreamQueryClient;
  private static String regionName;
  private static String databaseName;

  // override queries by configuration
  private void initQueries() {
    System.out.println("[Startup] initializing operations for AwsTimestreamClient");
    for(OperationType op : OperationType.values()) {
      String rawQuery = OPERATION_MAP.get(op);
      String extQuery = getProperties().getProperty("timestream.queries." + op.toString().toLowerCase(), rawQuery);
      OPERATION_MAP.put(op, extQuery);
      System.out.println("[Startup] " + op + ": " + extQuery);
    }
  }

  private static void initTable(Properties props) {
    final String tableName = props.getProperty("timestream.table", "");
    // The WriteClient and its config are only needed for the table creation
    final int maxConnections = Integer.parseInt(props.getProperty("timestream.maxconnections", "10"));
    final int numRetries = Integer.parseInt(props.getProperty("timestream.writeRetries", "10"));
    final int apiTimeout = Integer.parseInt(props.getProperty("timestream.apitimeout", "100"));
    ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
    httpClientBuilder.maxConnections(maxConnections);

    RetryPolicy.Builder retryPolicy = RetryPolicy.builder();
    retryPolicy.numRetries(numRetries);

    ClientOverrideConfiguration.Builder overrideConfig =
        ClientOverrideConfiguration.builder();
    overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(apiTimeout));
    overrideConfig.retryPolicy(retryPolicy.build());

    TimestreamWriteClient timestreamWriteClient = TimestreamWriteClient.builder()
        .httpClientBuilder(httpClientBuilder)
        .overrideConfiguration(overrideConfig.build())
        .region(Region.of(regionName))
        .build();
    try {
      // Create Table
      final RetentionProperties retentionProperties = RetentionProperties.builder()
          .memoryStoreRetentionPeriodInHours(1L)
          .magneticStoreRetentionPeriodInDays(1L).build();
      final CreateTableRequest createTableRequest = CreateTableRequest.builder()
          .databaseName(databaseName).tableName(tableName).retentionProperties(retentionProperties).build();
      timestreamWriteClient.createTable(createTableRequest);
    } catch(TimestreamWriteException ex) {
      System.err.println("table could not be created");
      ex.printStackTrace(System.err);
    }
  }

  @Override
  public void init() {
    synchronized (INCLUDE) {
      super.init();
      if (timestreamQueryClient != null) {
        return;
      }
      // Authentication: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
      // aws.accessKeyId, aws.secretKey, and aws.sessionToken
      Properties props = getProperties();
      final String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
      final String secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
      databaseName = props.getProperty("timestream.database", "");
      regionName = props.getProperty("timestream.region", "");
      initQueries();
      System.setProperty("aws.accessKeyId", accessKey);
      System.setProperty("aws.secretAccessKey", secretAccessKey);
      final boolean initTable = Boolean.parseBoolean(props.getProperty("timestream.inittable", "false"));
      if(initTable) {
        initTable(props);
      }
      timestreamQueryClient = TimestreamQueryClient.builder()
          .region(Region.of(regionName))
          .build();
    }
  }

  private String durationToTimestreamString(Duration timeInterval) {
    return timeInterval.toMinutes() + "m";
  }

  @Override
  public final Status tsQuery(OperationType op, Duration duration, Vector<HashMap<String, ByteIterator>> result) {
    String rawQuery = OPERATION_MAP.get(op);
    if(rawQuery == null) {
      System.err.println("query " + op + " not found");
      return Status.NOT_FOUND;
    }
    final String timeInterval = durationToTimestreamString(duration);
    String sql = String.format(rawQuery, databaseName, timeInterval);
    if(doDebug) {
      debug("querying (" + op + "): " + sql);
    }
    try {
      QueryRequest queryRequest = QueryRequest.builder().queryString(sql).build();
      try (Stream<QueryResponse> stream = timestreamQueryClient.queryPaginator(queryRequest).stream()) {
        // this is much like in the Influx case; be aware that this is lazy loading which may harm performance
        debug("query completed");
        stream.forEach(response -> {
          if(doDebug) {
            debug("stream status: " + response.queryStatus());
          }
          parseQueryResult(response, result);
          // response.rows().forEach(row -> System.err.printf("results: " + row.toString() + "/"));
        });
      }
      return Status.OK;
    } catch (Exception e) {
        System.err.println("query failed could not be created");
        e.printStackTrace(System.err);
    }
    return Status.ERROR;
  }

  private void parseQueryResult(QueryResponse response, Vector<HashMap<String, ByteIterator>> result) {
    List<ColumnInfo> columnInfo = response.columnInfo();
    List<Row> rows = response.rows();
    // iterate every row
    for (Row row : rows) {
      HashMap<String, ByteIterator> parsed = parseRow(columnInfo, row);
      result.add(parsed);
    }
    if(doDebug) {
      final QueryStatus currentStatusOfQuery = response.queryStatus();

      debug("Query progress so far: " + currentStatusOfQuery.progressPercentage() + "%");

      double bytesScannedSoFar = ((double) currentStatusOfQuery.cumulativeBytesScanned() / ONE_GB_IN_BYTES);
      debug("Bytes scanned so far: " + bytesScannedSoFar + " GB");

      double bytesMeteredSoFar = ((double) currentStatusOfQuery.cumulativeBytesMetered() / ONE_GB_IN_BYTES);
      debug("Bytes metered so far: " + bytesMeteredSoFar + " GB");

      debug("Metadata: " + columnInfo);
      debug("Data: ");
      result.forEach(row -> debug("\t\t data: " + row.toString()));
    }
  }

  private HashMap<String, ByteIterator> parseRow(List<ColumnInfo> columnInfo, Row row) {
    HashMap<String, ByteIterator> parsed = new HashMap<>();
    List<Datum> data = row.data();
    // iterate every column per row
    for (int j = 0; j < data.size(); j++) {
      ColumnInfo info = columnInfo.get(j);
      String value = parseDatum(info, data.get(j));
      parsed.put(info.name(), new StringByteIterator(value));
    }
    return parsed;
    // String.format("{%s}", rowOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
  }

  private String parseDatum(ColumnInfo info, Datum datum) {
    if (datum.nullValue() != null && datum.nullValue()) {
      return "<nil>";
    }
    Type columnType = info.type();
    // If the column is of TimeSeries Type
    if (columnType.timeSeriesMeasureValueColumnInfo() != null) {
      return parseTimeSeries(info, datum);
    }
    // If the column is of Array Type
    else if (columnType.arrayColumnInfo() != null) {
      List<Datum> arrayValues = datum.arrayValue();
      return info.name() + "=" + parseArray(info.type().arrayColumnInfo(), arrayValues);
    }
    // If the column is of Row Type
    else if (columnType.rowColumnInfo() != null && columnType.rowColumnInfo().size() > 0) {
      List<ColumnInfo> rowColumnInfo = info.type().rowColumnInfo();
      Row rowValues = datum.rowValue();
      throw new RuntimeException("unexpected column type: row");
      // return parseRow(rowColumnInfo, rowValues);
    }
    // If the column is of Scalar Type
    else {
      return parseScalarType(info, datum);
    }
  }
  private String parseScalarType(ColumnInfo info, Datum datum) {
    return datum.scalarValue();
  }
  private String parseTimeSeries(ColumnInfo info, Datum datum) {
    List<String> timeSeriesOutput = new ArrayList<>();
    for (TimeSeriesDataPoint dataPoint : datum.timeSeriesValue()) {
      timeSeriesOutput.add("{time=" + dataPoint.time() + ", value=" +
          parseDatum(info.type().timeSeriesMeasureValueColumnInfo(), dataPoint.value()) + "}");
    }
    return String.format("[%s]", timeSeriesOutput.stream().map(Object::toString)
        .collect(Collectors.joining(",")));
  }

  private String parseArray(ColumnInfo arrayColumnInfo, List<Datum> arrayValues) {
    List<String> arrayOutput = new ArrayList<>();
    for (Datum datum : arrayValues) {
      arrayOutput.add(parseDatum(arrayColumnInfo, datum));
    }
    return String.format("[%s]", arrayOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
  }
}

/**
        // Write
        System.out.println("Writing records");
        // Specify repeated values for all records
        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = Dimension.builder().name("region").value("us-east-1").build();
        final Dimension az = Dimension.builder().name("az").value("az1").build();
        final Dimension hostname = Dimension.builder().name("hostname").value("host1").build();

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record cpuUtilization = Record.builder()
            .dimensions(dimensions)
            .measureValueType(String.valueOf(MeasureValueType.DOUBLE))
            .measureName("cpu_utilization")
            .measureValue("13.5")
            .time(String.valueOf(time)).build();

        Record memoryUtilization = Record.builder()
            .dimensions(dimensions)
            .measureValueType(String.valueOf(MeasureValueType.DOUBLE))
            .measureName("memory_utilization")
            .measureValue("40")
            .time(String.valueOf(time)).build();

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
            .databaseName(databaseName).tableName(tableName).records(records).build();

        try {
          WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
          System.out.println("WriteRecords Status: " + writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
          System.out.println("RejectedRecords: " + e);
          for (RejectedRecord rejectedRecord : e.rejectedRecords()) {
            System.out.println("Rejected Index " + rejectedRecord.recordIndex() + ": "
                + rejectedRecord.reason());
          }
          System.out.println("Other records were written successfully. ");
        } catch (Exception e) {
          System.out.println("Error: " + e);
        }
 */