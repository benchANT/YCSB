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

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.WellKnownDataSet;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.TelegrafDB;
import site.ycsb.workloads.TelegrafWorkload.OperationType;

/**
 * Azure Data Explorer binding for YCSB framework using the Azure Data explorer <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 */
public final class AzureDataExplorerClient extends TelegrafDB {
  private static final Object INCLUDE = new Object();
  private final static Map<OperationType, String> OPERATION_MAP = new HashMap<>();
  static {
    // OPERATION_MAP.put(OperationType.Q01, "select count(*) from \"%1$s\".procstat where time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q01, "procstat | where timestamp > now() - %2$s | count");
    // OPERATION_MAP.put(OperationType.Q02, "select count(write_count), count(process_name), count(ppid), count(rlimit_memory_vms_hard) from \"%1$s\".procstat where time > now() - %2$s and write_bytes > 20000");
    OPERATION_MAP.put(OperationType.Q02, "procstat | where timestamp > now() - %2$s and fields.write_bytes " + Q02_WRITE_BYTES_FILTER +
                                            "| summarize writeCount=countif(notnull(fields.write_count))," +
                                            "processName=countif(notnull(tags.process_name))," +
                                            "ppid=countif(notnull(fields.ppid)),"+
                                            "rLimit=countif(notnull(fields.rlimit_memory_vms_hard))");
    // OPERATION_MAP.put(OperationType.Q03, "SELECT * FROM \"%1$s\".procstat where cpu_time > 85 and time > (now() - %2$s) order by time desc limit 5");
    OPERATION_MAP.put(OperationType.Q03, "procstat | where fields.cpu_time " + Q03_CPU_TIME_FILTER + " and timestamp > now() - %2$s | sort by timestamp desc | take 5");
    // OPERATION_MAP.put(OperationType.Q04, "SELECT count(*) FROM \"%1$s\".procstat where pid like '%%44%%' and time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q04, "procstat | where (timestamp > now() - %2$s) and (tags.pid contains \"44\") | count");
    // OPERATION_MAP.put(OperationType.Q05, "select * from \"%1$s\".procstat where time > now() - %2$s limit 10");
    OPERATION_MAP.put(OperationType.Q05, "procstat | where timestamp > now() - %2$s | take 10");
    // OPERATION_MAP.put(OperationType.Q06, "select distinct(memory_data) from \"%1$s\".procstat where cpu_time > 90 and time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q06, "procstat | where fields.cpu_time " + Q06_CPU_TIME_FILTER + " and timestamp > now() - %2$s | project memory=fields.memory_data | distinct tostring(memory)");
    // OPERATION_MAP.put(OperationType.Q07, "SELECT count(read_bytes), count(read_count) FROM \"%1$s\".procstat where num_threads > 20 and time > now() - %2$s Group by instance");
    OPERATION_MAP.put(OperationType.Q07, "procstat | where fields.num_threads " + Q07_THREADS_FILTER + " and timestamp > now() - %2$s | summarize readBytes=countif(notnull(fields.read_bytes)), readCount=countif(notnull(fields.read_count)) by tostring(tags.instance)");
    // OPERATION_MAP.put(OperationType.Q08, "select sum(rlimit_num_fds_soft), sum(rlimit_realtime_priority_hard), sum(rlimit_realtime_priority_soft), sum(rlimit_signals_pending_hard) from \"%1$s\".procstat where time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q08, "procstat | where timestamp > now() - %2$s | summarize sum(tolong(fields.rlimit_num_fds_soft)), sum(tolong(fields.rlimit_realtime_priority_hard)), sum(tolong(fields.rlimit_realtime_priority_soft)), sum(tolong(fields.rlimit_signals_pending_hard))");
    // OPERATION_MAP.put(OperationType.Q09, "SELECT sqrt(avg(cpu_time)), sin(avg(cpu_time)), cos(avg(cpu_time)), pow(avg(cpu_time),56456), round(avg(cpu_time)) FROM \"%1$s\".procstat where time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q09, "procstat | where timestamp > now() - %2$s | summarize sqrt=sqrt(avg(todouble(fields.cpu_time))), sin=sin(avg(todouble(fields.cpu_time))), cos=cos(avg(todouble(fields.cpu_time))),  pow=pow(avg(todouble(fields.cpu_time)),56456), round=round(avg(todouble(fields.cpu_time)))");
    // OPERATION_MAP.put(OperationType.Q10, "SELECT avg(minor_faults), memory_usage FROM \"%1$s\".procstat where cpu_time > 90 and time > now() - %2$s Group by memory_usage having memory_usage >= 0.05");
    OPERATION_MAP.put(OperationType.Q10, "procstat | where  timestamp > now() - %2$s and tolong(fields.cpu_time) " + Q10_CPU_TIME_FILTER + " | summarize avg(tolong(fields.minor_faults)) by memory=todouble(fields.memory_usage) | where memory " + Q10_MEMORY_FILTER);
    // OPERATION_MAP.put(OperationType.Q11, "SELECT min(memory_usage) FROM \"%1$s\".procstat where cpu_time > 80 and time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q11, "procstat | where timestamp > now() - %2$s and notnull(fields.memory_usage) and notnull(fields.cpu_time) and tolong(fields.cpu_time) " + Q11_CPU_TIME_FILTER + " | summarize min(todouble(fields.memory_usage))");
    // OPERATION_MAP.put(OperationType.Q12, "Q12: select max(cpu_time) from \\\"%1$s\\\".procstat where memory_usage > 0.1 and time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q12, "procstat | where timestamp > now() - %2$s and isnotnull(fields.cpu_time) and todouble(fields.memory_usage) " + Q12_MEMORY_USAGE_FILTER + " | summarize max(todouble(fields.cpu_time))");
    // OPERATION_MAP.put(OperationType.Q13, "select min(memory_usage) from \"%1$s\".procstat where cpu_time > 80 and time > now() - %2$s Union ALL select max(cpu_time) from \"%1$s\".procstat where memory_usage > 0.1 and time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q13, 
      "procstat"
          + "| where timestamp > now() - %2$s and fields.cpu_time " + Q13_CPU_TIME_FILTER
          + "| summarize min_memory=min(todouble(fields.memory_usage))"
          + "| union (procstat"
          +     "| where timestamp > now() - %2$s and fields.memory_usage " + Q13_MEMORY_USAGE_FILTER
          +     "| summarize max_cpu=max(tolong(fields.cpu_time))"
          + ")");
    // OPERATION_MAP.put(OperationType.Q14, "select count(cpu_time) + sum(cpu_time) from \"%1$s\".procstat where time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q14, "procstat | where timestamp > now() - %2$s and isnotnull(fields.cpu_time) | summarize count(fields.cpu_time) + sum(tolong(fields.cpu_time))");
    //OPERATION_MAP.put(OperationType.Q15, "select sum(write_count) from \"%1$s\".procstat where write_bytes > 2000000 and instance in ('instance-00', 'instance-01', 'instance-02') and time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q15, "procstat |  where timestamp > now() - %2$s and fields.write_bytes "  + Q15_WRITE_BYTES_FILTER + " and tags.instance in ('instance-0-0', 'instance-0-1', 'instance-0-2') | summarize sum(tolong(fields.write_count))");
    // OPERATION_MAP.put(OperationType.Q16, "select count(write_count) from \"%1$s\".procstat where (instance ='instance-05' or instance ='instance-06') and time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q16, "procstat | where timestamp > now() - %2$s and isnotnull(fields.write_count) and (tags.instance == 'instance-0-5' or tags.instance == 'instance-0-6') | summarize count(tolong(fields.write_count))");
    // OPERATION_MAP.put(OperationType.Q17, "SELECT all(child_major_faults) FROM \"%1$s\".procstat where instance ='instance-08' and time > now() - %2$s");
    OPERATION_MAP.put(OperationType.Q17, "procstat | where timestamp > now() - %2$s and tags.instance == 'instance-0-8' | project fields.child_major_faults");
    // OPERATION_MAP.put(OperationType.Q18, "SHOW TABLES FROM %1$s");
    OPERATION_MAP.put(OperationType.Q18, ".show tables");
    // OPERATION_MAP.put(OperationType.Q19, "select date_trunc('second', time) as time_bucket, max(time), count(*) from procstat where time > now() - interval '%s' group by time_bucket order by time_bucket");
    OPERATION_MAP.put(OperationType.Q19, "procstat | where timestamp > now() - %2$s | extend time_bucket=bin(timestamp, 1s) | summarize max(timestamp), count() by time_bucket | sort by time_bucket asc");
    // OPERATION_MAP.put(OperationType.Q20, "SELECT median(write_count) FROM procstat where (instance ='instance-01' or instance ='instance-02') and time > now() - interval '%s'");
    OPERATION_MAP.put(OperationType.Q20, "procstat | where timestamp > now() - %2$s and (tags.instance == 'instance-0-1' or tags.instance == 'instance-0-2') | summarize percentile(todouble(fields.memory_usage), 50)");
    // OPERATION_MAP.put(OperationType.Q21, "select date_bin(interval '15 minute', time, timestamp '1970-01-01T00:00:00Z') as binned, mean(\"cpu_time\") as value, approx_percentile_cont(\"cpu_time\", 0.99) as perc, coalesce(instance, 'instance-74') as instance from procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' and time > now() - interval '%s' group by instance,binned");
    OPERATION_MAP.put(OperationType.Q21, "procstat " +
                                        " | where timestamp > now() - %2$s and tags.process_name contains \"kworker\" and tags.instance != 'instance-75' "+
                                        " | extend binned=bin_at(timestamp, 15m, datetime('1970-01-01T00:00:00Z')), instance=coalesce(tags.instance, 'instance-74')" +
                                        " | summarize value=avg(tolong(fields.cpu_time)), prec=percentile(tolong(fields.cpu_time), 99)" + 
                                        " by instance, binned");
    // OPERATION_MAP.put(OperationType.Q22, 
      // "select binned as time, value/1000000000 as mean, perc as p99, 'duration' as _field, instance from
      // ( select date_bin(interval '15 minute', time, timestamp '1970-01-01T00:00:00Z') as binned,
      //  mean(\"cpu_time\") as value,
      //  approx_percentile_cont(\"cpu_time\", 0.99) as perc,
      //  coalesce(instance, 'instance-74') as instance from procstat
      //    where \"process_name\" like '%%kworker%%' and instance != 'instance-75' and time > now() - interval '%s' group by instance,binned)
      // ORDER BY time ASC");
    OPERATION_MAP.put(OperationType.Q22,
        "procstat | where timestamp > now() - %2$s and tags.process_name contains \"kworker\" and tags.instance != 'instance-75' "
      + "| extend binned=bin_at(timestamp, 15m, datetime('1970-01-01T00:00:00Z')), instance=coalesce(tags.instance, 'instance-74')" 
      + "| summarize value=avg(tolong(fields.cpu_time)), perc=percentile(tolong(fields.cpu_time), 99) by instance, binned"
      + "| project ['time']=binned, mean=value/1000000000, p99=perc, _field='duration', instance | sort by ['time'] asc");
    // OPERATION_MAP.put(OperationType.Q23, "SELECT binned as time, mean, p99 FROM ( select date_bin(interval '15 minute', time, timestamp '1970-01-01T00:00:00Z') as binned, mean(\"cpu_time\") as mean, approx_percentile_cont(\"cpu_time\", 0.99) as p99 from procstat where \"process_name\" like '%%kworker%%' and instance != 'instance-75' AND time > now() - interval '%s' GROUP BY binned) ORDER BY binned ASC");
    OPERATION_MAP.put(OperationType.Q23, 
        "procstat | where timestamp > now() - %2$s and tags.process_name contains \"kworker\" and tags.instance != 'instance-0-75' "
      + "| extend binned=bin_at(timestamp, 15m, datetime('1970-01-01T00:00:00Z'))"
      + "| summarize mean=avg(tolong(fields.cpu_time)), p99=percentile(tolong(fields.cpu_time), 99) by binned"
      + "| project ['time']=binned, mean, p99 | sort by ['time'] asc");
    // OPERATION_MAP.put(OperationType.Q24, "SELECT \"cpu_time_system\", \"cpu_time\", \"process_name\", \"cpu_time_user\" FROM procstat WHERE \"instance\" = 'instance-75' AND time >= now() - interval  '%s' ORDER BY \"process_name\" DESC");
    OPERATION_MAP.put(OperationType.Q24, "procstat | where timestamp > now() - %2$s and tags.instance == 'instance-0-11' | project fields.cpu_time_system, fields.cpu_time, process_name=tostring(tags.process_name), fields.cpu_time_user | sort by process_name desc");
    // OPERATION_MAP.put(OperationType.Q25, "select binned as time, value as _value, \"process_name\", 'cpu_time' as _field from 
    //      (select date_bin(interval '%1$s', time, timestamp '1970-01-01T00:00:00Z') as binned, count(\"instance\") as value, \"process_name\" from procstat 
    //      where \"process_name\" like '%%kworker%%' and time > now() - interval '%1$s' group by \"process_name\", binned) order by time");   
    OPERATION_MAP.put(OperationType.Q25, 
      "procstat"
        + "| where timestamp > now() - %2$s and tags.process_name contains \"kworker\""
        + "| extend binned=bin_at(timestamp, %2$s, datetime('1970-01-01T00:00:00Z'))"
        + "| summarize value=count(tags.instance) by process_name=tostring(tags.process_name), binned"
        + "| project [\"time\"]=binned, _value=value, process_name, _field='cpu_time'"
        + "| sort by [\"time\"]");   
    // OPERATION_MAP.put(OperationType.Q26, "select binned as time, value as _value, process_name, CASE process_name WHEN 'telegraf' THEN '01-telegraf' WHEN 'dockerd' THEN '02-dockerd' WHEN 'containerd' THEN '03-containerd' WHEN 'bash' THEN '04 - bash' WHEN 'systemd-logind' THEN '05 - systemd-logind' WHEN 'pgrep' THEN '06 - pgrep' WHEN 'cron' THEN '07 - cron' WHEN 'snapd' THEN '08 - snapd' else process_name END as _field from (select date_bin(interval '%1$s', time, timestamp '1970-01-01T00:00:00Z') as binned, count(\"cpu_time\") as value, \"process_name\" from procstat where \"host\" like '%%client%%' and process_name != 'systemd-networkd' and process_name != 'systemd' and time > (now() - interval '%1$s') group by \"process_name\", binned) order by time");
    OPERATION_MAP.put(OperationType.Q26, 
      "procstat"
        + "| where timestamp > now() - %2$s and tags.process_name != 'systemd-networkd' and  tags.process_name != 'systemd' and tags.host contains \"benchmark\""
        + "| extend binned=bin_at(timestamp, %2$s, datetime('1970-01-01T00:00:00Z'))"
        + "| summarize value=countif(isnotnull(fields.cpu_time)) by process_name=tostring(tags.process_name), binned"
        + "| project [\"time\"]=binned, _value=value, process_name, _field= case ("
              + "process_name == 'telegraf', '01-telegraf',"
              + "process_name == 'dockerd', '02-dockerd',"
              + "process_name == 'containerd', '03-containerd',"
              + "process_name == 'bash', '04 - bash',"
              + "process_name == 'systemd-logind', '05 - systemd-logind',"
              + "process_name == 'pgrep', '06 - pgrep',"
              + "process_name == 'cron', '07 - cron',"
              + "process_name == 'snapd', '08 - snapd',"
              + "process_name)"
        + "| sort by [\"time\"]");
    // OPERATION_MAP.put(OperationType.Q27, "SELECT sum(\"_value\") as \"_value\" FROM (SELECT sum(\"_value\") as \"_value\", \"instance\" FROM (SELECT max(cpu_time) as \"_value\", instance, process_name FROM procstat where \"process_name\" like '%%kworker%%' AND time > (now() - interval '%s') GROUP BY \"instance\", \"process_name\" ) GROUP BY \"instance\")");
    OPERATION_MAP.put(OperationType.Q27, 
      "procstat"
        + "| where timestamp > now() - %2$s and tags.process_name contains \"kworker\""
        + "| summarize _value=max(tolong(fields.cpu_time)) by instance=tostring(tags.instance), process_name=tostring(tags.process_name)"
        + "| summarize _value=sum(_value) by instance"
        + "| summarize _value=sum(_value)");
  }
  private static Client client;
  private String databaseName;

  private void initTable(String database) {
    try {
      String createTableCommand = ".set-or-replace  testTable <|\n" +
          "range x from 1 to 100 step 1\n" +
          "| extend ts = totimespan(strcat(x,'.00:00:00'))\n" +
          "| project timestamp = now(ts), eventName = strcat('event ', x)";
      client.executeQuery(database, createTableCommand, null);
      // create Table and write 100 rows
      client.execute(database, createTableCommand);  
    } catch (DataServiceException | DataClientException e) {
      System.err.println("could not init table. aborting ...");
      e.printStackTrace(System.err);
      System.exit(-1);
    }
  }
  // override queries by configuration
  private void initQueries() {
    System.out.println("[Startup] initializing operations for AzureDataExplorerClient");
    for(OperationType op : OperationType.values()) {
      String rawQuery = OPERATION_MAP.get(op);
      String extQuery = getProperties().getProperty("dataexplorer.queries." + op.toString().toLowerCase(), rawQuery);
      OPERATION_MAP.put(op, extQuery);
      System.out.println("[Startup] " + op + ": " + extQuery);
    }
  }

  @Override
  public void init() {
    synchronized (INCLUDE) {
      super.init();
      if (client != null) {
        return;
      }
      Properties props = getProperties();
      databaseName = props.getProperty("dataexplorer.database", "");
      String endpoint = props.getProperty("dataexplorer.endpoint", "");
      String tenantID = System.getenv("AZURE_TENANT");
      String clientID = System.getenv("AZURE_CLIENT_ID");
      String clientSecret = System.getenv("AZURE_SECRET");
      ConnectionStringBuilder csb = ConnectionStringBuilder
        .createWithAadApplicationCredentials(endpoint, clientID, clientSecret, tenantID);
      try {
        client = ClientFactory.createClient(csb);
      } catch (URISyntaxException e) {
        System.err.println("cannot initialize client.");
        e.printStackTrace(System.err);
        System.exit(-1);
      }
      final boolean initTable = Boolean.parseBoolean(props.getProperty("dataexplorer.inittable", "false"));
      if(initTable) {
        initTable(databaseName);
      }
      initQueries();
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
    // String sql = "--\n" + String.format(rawQuery, databaseName, timeInterval);
    String sql = String.format(rawQuery, databaseName, timeInterval);
    debug("querying (" + op + "): " + sql);
    try {
      // QueryRequest queryRequest = QueryRequest.builder().queryString(sql).build();
      KustoOperationResult readResults = client.execute(databaseName, sql);
      debug("query ended");
      final KustoResultSetTable pR = readResults.getPrimaryResults();
      final KustoResultColumn[] cs = pR.getColumns();
      pR.getData().forEach(row -> {
        HashMap<String, ByteIterator> parsed = new HashMap<>();
        for(int i = 0; i < row.size(); i++) {
          Object data = row.get(i);
          if(null == data) {
            parsed.put(cs[i].getColumnName(), new StringByteIterator("<nil>"));
          } else {
            parsed.put(cs[i].getColumnName(), new StringByteIterator(data.toString()));  
          }
          // parsed.put(, new StringByteIterator(row.get(i).toString()));
        }
        result.add(parsed);
      });  
      if(doDebug) {     
        while(readResults.hasNext()){
          final KustoResultSetTable r = readResults.next();
          if(r.getTableKind() == WellKnownDataSet.PrimaryResult) continue;
          debug("table name / kind: " + r.getTableName() + "/" + r.getTableKind());
          final String[] cNames = new String[r.getColumns().length];
          for(int i = 0; i < cNames.length; i++) {
            cNames[i] = r.getColumns()[i].getColumnName();
          }
          debug("\t columns: " + Arrays.toString(cNames));
          r.getData().forEach(row -> debug("\t\t data " +  Arrays.toString(row.toArray())));
        }
        debug("\n\tprimary results");
        result.forEach(row -> debug("\t\t data: " + row.toString()));
        // debug("result: ", result);
      }
      return Status.OK;
    } catch (Exception e) {
        System.err.println("query failed could not be created");
        e.printStackTrace(System.err);
    }
    return Status.ERROR;
  }
}
