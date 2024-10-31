/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 * Copyright (c) 2024 benchANT GmbH. All rights reserved.
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

package site.ycsb.db;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
// import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ReadModeAP;
// import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.IAerospikeQueryDB;
import site.ycsb.NumericByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.datamodel.DataModelEntry;
import site.ycsb.datamodel.DataModelRegistry;
import site.ycsb.workloads.CoreWorkload;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Areospike</a>.
 */
public class Aerospike7Client extends site.ycsb.DB implements IAerospikeQueryDB {
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static final String HOSTS_PROPERTY = "as.host";
  private static final String USER_PROPERTY = "as.user";
  private static final String NAMESPACE_PROPERTY = "as.namespace";
  private static final String PASSWORD_PROPERTY = "as.password";
  private static final String PORT_PROPERTY = "as.port";
  private static final String READ_MODE_AP_PROPERTY = "as.readmodeap";
  private static final String CONNECTIONS_PER_NODE_PROPERTY = "as.connspernode";
  private static final String CONNECTION_POOLS_PER_NODE_PROPERTY = "as.connpoolspernode";
  private static final String TOTAL_TIMEOUT_PROPERTY = "as.totaltimeout";
  // private static final String MAX_RETRIES_PROPERTY = "as.maxretries";
  private static final String WRITE_COMMIT_LEVEL_PROPERTY = "as.writecommitlevel";
  private static final String DURABLE_DELETE_PROPERTY = "as.durabledelete";
  private static final String USE_COMPRESSION_PROPERTY = "as.usecompression";
  // private static final String COMPRESSION_STRATEGY_PROPERTY = "as.compressionstrategy";
  private static final String DEBUG_PROPERTY = "as.debug";

  private static final int DEFAULT_BATCH_SIZE = 0;
  private static final int DEFAULT_MAX_RETRIES = 0;
  // private static final String DEFAULT_INSERT_STRATEGY = "CREATE_ONLY";
  // private static final String DEFAULT_COMPRESSION_STRATEGY = "zlib";
  private static final String DEFAULT_DURABLE_DELETE_STRATEGY = "true";

  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";

  private static String namespace = null;

  private static AerospikeClient client = null;
  /** The batch size to use for inserts. */
  private static int batchSize = 0;

  private static final Policy readPolicy = new Policy();
  private static QueryPolicy queryPolicy;
  private static final BatchWritePolicy batchInsertPolicy = new BatchWritePolicy();
  private static WritePolicy insertPolicy;
  private static WritePolicy updatePolicy;
  private static final WritePolicy deletePolicy = new WritePolicy();
  private static boolean useDebug;

  private void initReadPolicy(Properties props) {
    readPolicy.setMaxRetries(DEFAULT_MAX_RETRIES);
    System.err.println("Aerospike binding read policy: max retries 0");
    int timeout = Integer.parseInt(props.getProperty(TOTAL_TIMEOUT_PROPERTY, DEFAULT_TIMEOUT));
    readPolicy.totalTimeout = timeout;
    System.err.println("Aerospike binding read policy: total timeout " + timeout);
    String readModeAP = props.getProperty(READ_MODE_AP_PROPERTY);
    if(readModeAP != null) {
      readPolicy.setReadModeAP(ReadModeAP.valueOf(readModeAP));
      System.err.println("Aerospike binding read policy: read mode " + readModeAP);
    }
    String useCompression = props.getProperty(USE_COMPRESSION_PROPERTY, "true");
    boolean isFalse = useCompression.toLowerCase().equals("false");
    readPolicy.setCompress(!isFalse);
    System.err.println("Aerospike binding read policy: compress " + (!isFalse));
  }

  private void initQueryPolicy(Properties props) {
    queryPolicy = new QueryPolicy(readPolicy);
  }

  private void initInsertPolicy(Properties props) {
    insertPolicy = new WritePolicy(readPolicy);
    insertPolicy.setRespondAllOps(true);
    String commitLevel = props.getProperty(WRITE_COMMIT_LEVEL_PROPERTY);
    if(commitLevel != null) {
      insertPolicy.setCommitLevel(CommitLevel.valueOf(commitLevel));
      System.err.println("Aerospike binding init policy: commit level " + commitLevel);
    }
    insertPolicy.setRecordExistsAction(RecordExistsAction.CREATE_ONLY);
    System.err.println("Aerospike binding init policy: record exists action" + RecordExistsAction.CREATE_ONLY);
  }

  private void initUpdatePolicy(Properties props) {
    updatePolicy = new WritePolicy(insertPolicy);
    // insertPolicy.setRecordExistsAction(RecordExistsAction.REPLACE_ONLY);
    insertPolicy.setRecordExistsAction(RecordExistsAction.REPLACE);
    System.err.println("Aerospike binding update policy: record exists action" + RecordExistsAction.REPLACE);
  }

  private void initDeletePolicy(Properties props) {
    updatePolicy = new WritePolicy(insertPolicy);
    String durableDelete = props.getProperty(DURABLE_DELETE_PROPERTY, DEFAULT_DURABLE_DELETE_STRATEGY);
    boolean isFalse = durableDelete.toLowerCase().equals("false");
    updatePolicy.setDurableDelete(!isFalse);
    System.err.println("Aerospike binding delete policy: durable delete" + (!isFalse));
  }

  private void initSecondaryIndexes(Properties props) {
    System.err.println("Aerospike binding: intializing secondary indexes, if any");
    String table = props.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    List<DataModelEntry> sis = DataModelRegistry.INSTANCE.findSiFields();
    for(DataModelEntry e : sis) {
      String name = e.getFieldName();
      IndexType myIdx = e.isNumeric() ? IndexType.NUMERIC : IndexType.STRING;
      String idx_name = name + "_idx";
      System.err.println("Aerospike binding: creating index " + idx_name + " for field " + name);
      client.createIndex(null, namespace, table, idx_name, name, myIdx);
    }
  }

  @Override
  public void init() throws DBException {
    // Keep track of number of calls to init (for later cleanup)
    // this is a bit of a stupid approach, but adapted from Cassandra binding
    INIT_COUNT.incrementAndGet();
    synchronized(INIT_COUNT){
      if(client != null) {
        return;
      }
      Properties props = getProperties();

      namespace = props.getProperty(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE);
      initReadPolicy(props);
      initInsertPolicy(props);
      initUpdatePolicy(props);
      initDeletePolicy(props);
      initQueryPolicy(props);

      String host = props.getProperty(HOSTS_PROPERTY, DEFAULT_HOST);
      String user = props.getProperty(USER_PROPERTY);
      String password = props.getProperty(PASSWORD_PROPERTY);
      int port = Integer.parseInt(props.getProperty(PORT_PROPERTY, DEFAULT_PORT));

      String connectionsPerNode = props.getProperty(CONNECTIONS_PER_NODE_PROPERTY);
      String connectionPoolsPerNode = props.getProperty(CONNECTION_POOLS_PER_NODE_PROPERTY);
      useDebug = Boolean.parseBoolean(props.getProperty(DEBUG_PROPERTY));

      ClientPolicy clientPolicy = new ClientPolicy();

      if (user != null && password != null) {
        clientPolicy.user = user;
        clientPolicy.password = password;
        System.err.println("Aerospike binding: client policy: " + user + " / *******");
      }
      if(connectionsPerNode != null) {
        int conns = Integer.parseInt(connectionsPerNode);
        clientPolicy.setMaxConnsPerNode(conns);
        clientPolicy.setMinConnsPerNode(conns);
        System.err.println("Aerospike binding: min / max connections per node: " + conns);
      }
      if(connectionPoolsPerNode != null) {
        int pools = Integer.parseInt(connectionPoolsPerNode);
        clientPolicy.setConnPoolsPerNode(pools);
        System.err.println("Aerospike binding: pools per node: " + pools);
      }
      try {
        client =
            new AerospikeClient(clientPolicy, host, port);
      } catch (AerospikeException e) {
        throw new DBException(String.format("Error while creating Aerospike " +
            "client for %s:%d.", host, port), e);
      }
      initSecondaryIndexes(props);
      batchSize = Integer.parseInt(props.getProperty("batchsize", Integer.toString(DEFAULT_BATCH_SIZE)));
      System.err.println("Aerospike binding: use batch size: " + batchSize);
    } // synchronized
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        client.close();
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));        
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      Record record;

      if (fields != null) {
        record = client.get(readPolicy, new Key(namespace, table, key),
            fields.toArray(new String[fields.size()]));
      } else {
        record = client.get(readPolicy, new Key(namespace, table, key));
      }

      if (record == null) {
        if(useDebug) {
          System.err.println("Record key " + key + " not found (read)");
        } 
        
        return Status.NOT_FOUND;
      }

      for (Map.Entry<String, Object> entry: record.bins.entrySet()) {
        DataModelEntry fieldMeta = DataModelRegistry.INSTANCE.getField(entry.getKey());
        if(fieldMeta != null && fieldMeta.isNumeric()){
          result.put(entry.getKey(), new NumericByteIterator((long) entry.getValue()));
        } else {
          result.put(entry.getKey(),
              new ByteArrayByteIterator((byte[])entry.getValue()));
        }
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while reading key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return Status.NOT_IMPLEMENTED;
  }

  private Status write(String table, String key, WritePolicy writePolicy,
      Map<String, ByteIterator> values) {
    Bin[] bins = new Bin[values.size()];
    int index = 0;

    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      DataModelEntry fieldMeta = DataModelRegistry.INSTANCE.getField(entry.getKey());
      if(fieldMeta != null && fieldMeta.isNumeric()){
        bins[index] = new Bin(entry.getKey(), ((NumericByteIterator) entry.getValue()).getLong());
      } else {
        bins[index] = new Bin(entry.getKey(), entry.getValue().toArray());
      }
      ++index;
    }

    Key keyObj = new Key(namespace, table, key);

    try {
      client.put(writePolicy, keyObj, bins);
      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while writing key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  private Status batchInsert(String table, String key, BatchWritePolicy batchWritePolicy) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, updatePolicy, values);
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    if(batchSize > 1) {
      return batchInsert(table, key, batchInsertPolicy);
    } else {
      return write(table, key, insertPolicy, values);
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      if (!client.delete(deletePolicy, new Key(namespace, table, key))) {
        if(useDebug) {
          System.err.println("Record key " + key + " not found (delete)");
        } 
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status query(String table, Map<String, ByteIterator> fields, List<Map<String, ByteIterator>> result) {
    if(fields.size() > 1) {
      throw new IllegalArgumentException("only one filter per query supported");
    }
    if(fields.size() < 1) {
      throw new IllegalArgumentException("one filter required per query");
    }
    Statement s = new Statement();
    s.setNamespace(namespace);
    s.setSetName(table);
    for(String field : fields.keySet()) {
      s.setFilter(Filter.equal(field, ((NumericByteIterator) fields.get(field)).getLong()));
    }
    // TODO: validate if this method is using the right approach or
    // or if another query method should be used instead
    RecordSet rs = client.query(queryPolicy, s);
    Iterator<KeyRecord> irecord = rs.iterator();
    while(irecord.hasNext()) {
      KeyRecord r = irecord.next();
      Map<String, Object> bins = r.record.bins;
      Map<String, ByteIterator> cResult = new HashMap<>();
      cResult.put("key", new StringByteIterator(r.key.toString()));
      for (Map.Entry<String, Object> entry: bins.entrySet()) {
        DataModelEntry fieldMeta = DataModelRegistry.INSTANCE.getField(entry.getKey());
        if(fieldMeta != null && fieldMeta.isNumeric()){
          cResult.put(entry.getKey(), new NumericByteIterator((long) entry.getValue()));
        } else {
          cResult.put(entry.getKey(),
              new ByteArrayByteIterator((byte[])entry.getValue()));
        }
      }
      result.add(cResult);
    }
    if(result.isEmpty()) {
      if(useDebug) {
        HashMap<String,Long> m = new HashMap<>();
        for(String field : fields.keySet()) {
          m.put(field, ((NumericByteIterator) fields.get(field)).getLong());
        }
        System.err.println("query not successful for Aerospike: " + m);
      }
      return Status.NOT_FOUND;
    }
    return Status.OK;
  }
}
