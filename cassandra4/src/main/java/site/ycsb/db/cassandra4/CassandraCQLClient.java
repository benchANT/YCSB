/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 * Copyright (c) 2024 benchANT GmbH. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
  */
package site.ycsb.db.cassandra4;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import site.ycsb.*;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Cassandra 2.x CQL client.
 * <p>
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(CassandraCQLClient.class);

  //  private static Cluster cluster = null;
  private static CqlSession session = null;

  private static ConcurrentMap<Set<String>, PreparedStatement> readStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> scanStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> insertStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> updateStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static AtomicReference<PreparedStatement> readAllStmt =
      new AtomicReference<PreparedStatement>();
  private static AtomicReference<PreparedStatement> scanAllStmt =
      new AtomicReference<PreparedStatement>();
  private static AtomicReference<PreparedStatement> deleteStmt =
      new AtomicReference<PreparedStatement>();

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";
  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";


  public static final String MAX_REQUESTS_PER_CONNECTION_PROPERTY =
      "cassandra.maxrequestsperconnection";
  public static final String MAX_CONNECTIONS_PER_NODE_PROPERTY =
      "cassandra.maxconnectionspernode";
  public static final String THREADS_PER_IO_GROUP_PROPERTY =
      "cassandra.threadsperiogroup";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.connecttimeoutmillis";
  public static final String REQUEST_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.requesttimeoutmillis";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.readconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.writeconsistencylevel";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static ConsistencyLevel readConsistency = null;
  private static ConsistencyLevel writeConsistency = null;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the session has already been initialized
      if (session != null) {
        return;
      }
      try {
        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);
        int fieldCount = Integer.parseInt(getProperties().getProperty("cassandra.table.columns", "10"));
        String path = getProperties().getProperty("cassandra.path");
        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY,
            KEYSPACE_PROPERTY_DEFAULT);
        String maxRequestsPerConnection = getProperties().getProperty(MAX_REQUESTS_PER_CONNECTION_PROPERTY);
        String maxConnectionsPerNode = getProperties().getProperty(MAX_CONNECTIONS_PER_NODE_PROPERTY);
        String threadsPerIoGroup = getProperties().getProperty(THREADS_PER_IO_GROUP_PROPERTY);
        String connectTimoutMillis = getProperties().getProperty(
            CONNECT_TIMEOUT_MILLIS_PROPERTY);
        String requestTimoutMillis = getProperties().getProperty(
            REQUEST_TIMEOUT_MILLIS_PROPERTY);
        String stringReadConsistency = getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
            DefaultConsistencyLevel.QUORUM.name());
        logger.info("setting read (and default) Consistency to '" + stringReadConsistency + "'");
        String stringWriteConsistency = getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
            DefaultConsistencyLevel.QUORUM.name());
        logger.info("setting write Consistency to '" + stringWriteConsistency + "'");
        writeConsistency = DefaultConsistencyLevel.valueOf(stringWriteConsistency);
        readConsistency = DefaultConsistencyLevel.valueOf(stringReadConsistency);
        boolean initDefaultTable = Boolean.parseBoolean(getProperties().getProperty("cassandra.initDefaultTable",
            "true"));
        boolean useSecureBundle = Boolean.parseBoolean(getProperties().getProperty("cassandra.useSecureBundle",
            "true"));
        //TODO: Check, if there is an equivalent for setCoreConnectionsPerHost
        ProgrammaticDriverConfigLoaderBuilder loader = DriverConfigLoader.programmaticBuilder();
        loader.withString(DefaultDriverOption.REQUEST_CONSISTENCY, stringReadConsistency);
        if (connectTimoutMillis != null) {
          logger.info("setting '" + DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT + "' to '" + connectTimoutMillis + "'");
          loader.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
              Duration.ofMillis(Integer.parseInt(connectTimoutMillis)));
        }
        if (maxRequestsPerConnection != null) {
          logger.info("setting '" + DefaultDriverOption.CONNECTION_MAX_REQUESTS + "' to '" + maxRequestsPerConnection + "'");
          loader.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, Integer.parseInt(maxRequestsPerConnection));
        }
        if (maxConnectionsPerNode != null) {
          logger.info("setting '" + DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE + "' to '" + maxConnectionsPerNode + "'");
          loader.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, Integer.parseInt(maxConnectionsPerNode));
          logger.info("setting '" + DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE + "' to '" + maxConnectionsPerNode + "'");
          loader.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, Integer.parseInt(maxConnectionsPerNode));
        }
        if(threadsPerIoGroup != null) {
          logger.info("setting '" + DefaultDriverOption.NETTY_IO_SIZE + "' to '" + threadsPerIoGroup + "'");
          loader.withInt(DefaultDriverOption.NETTY_IO_SIZE, Integer.parseInt(threadsPerIoGroup));
        }
        if (requestTimoutMillis != null) {
          logger.info("setting '" + DefaultDriverOption.REQUEST_TIMEOUT + "' to '" + requestTimoutMillis + "'");
          loader.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
              Duration.ofMillis(Integer.parseInt(requestTimoutMillis)));
        }

        if (useSecureBundle) {
          session = CqlSession.builder()
              .withCloudSecureConnectBundle(Paths.get(path))
              .withAuthCredentials(username, password)
              .withKeyspace(keyspace)
              .withConfigLoader(loader.build())
              .build();
        } else {
          String host = getProperties().getProperty(HOSTS_PROPERTY);
          if (host == null) {
            throw new DBException(String.format(
                "Required property \"%s\" missing for CassandraCQLClient",
                HOSTS_PROPERTY));
          }
          String[] hosts = host.split(",");
          int port = Integer.valueOf(getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT));
          List<InetSocketAddress> contactPoints = Stream.of(hosts)
              .map(x -> new InetSocketAddress(x, port)).collect(Collectors.toList());
         
          session = CqlSession.builder()
              .addContactPoints(contactPoints)
              .withLocalDatacenter("datacenter1")
              .withAuthCredentials(username, password)
              .withKeyspace(keyspace)
              .withConfigLoader(loader.build())
              .build();
        }

        session.getMetadata();
        Metadata metadata = session.getMetadata();
        logger.info("Connected to cluster: {}\n",
            metadata.getClusterName());
        if (initDefaultTable) {
          StringBuilder fields = new StringBuilder();
          for (int i = 0; i < fieldCount; i++) {
            fields.append(", field").append(i).append(" varchar");
          }
          fields.append(");");
          session.execute(
              "CREATE TABLE IF NOT EXISTS usertable (y_id varchar primary key" + fields
          );
          logger.info("Creating table with command: 'CREATE TABLE IF NOT EXISTS usertable (y_id varchar primary key"
              + fields + "'");
        }
      } catch (Exception e) {
        throw new DBException(e);
      }
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        readStmts.clear();
        scanStmts.clear();
        insertStmts.clear();
        updateStmts.clear();
        readAllStmt.set(null);
        scanAllStmt.set(null);
        deleteStmt.set(null);
        session.close();
        session = null;
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = (fields == null) ? readAllStmt.get() : readStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Select query;
        if (fields == null) {
          query = selectFrom(table).all();
        } else {
          query = selectFrom(table).columns(fields.toArray(new String[fields.size()]));
        }
        query = query.whereColumn(YCSB_KEY).isEqualTo(bindMarker());
        query.limit(1);
        SimpleStatement ss = query.build();
        ss.setConsistencyLevel(readConsistency);
        stmt = session.prepare(ss);
        // TODO: Tracing does not exist any more?
        PreparedStatement prevStmt = (fields == null)
                              ? readAllStmt.getAndSet(stmt)
                              : readStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      logger.debug(stmt.getQuery());
      logger.debug("key = {}", key);
      
      ResultSet rs = session.execute(stmt.bind(key));
      // Should be only 1 row
      Row row = rs.one();
      if (row == null) {
        return Status.NOT_FOUND;
      }
      ColumnDefinitions cds = rs.getColumnDefinitions();
      for(ColumnDefinition c : cds) {
        DataType myType = c.getType();
        if(DataTypes.TEXT.equals(myType)) {
          String cName = c.getName().toString();
          String value = row.getString(cName);
          result.put(cName, new StringByteIterator(value));
        } else {
          logger.error("unexpected type: " + myType);
          return Status.ERROR;
        }        
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * <p>
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      if(fields.size() == 0) {
        return Status.UNEXPECTED_STATE;
      }
      PreparedStatement stmt = updateStmts.get(fields);
      // Prepare statement on demand
      if (stmt == null) {
        Iterator<String> it = fields.iterator();
        UpdateWithAssignments updateQuery = QueryBuilder.update(table).setColumn(it.next(), bindMarker());
        // Add fields
        while (it.hasNext()) {
          updateQuery = updateQuery.setColumn(it.next(), bindMarker());
        }
        SimpleStatement ss = updateQuery.whereColumn(YCSB_KEY).isEqualTo(bindMarker()).build();
        ss.setConsistencyLevel(writeConsistency);
        stmt = session.prepare(ss);
        PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      if (logger.isDebugEnabled()) {
        logger.error(stmt.getQuery());
        logger.error("key = {}", key);
        logger.error("fields = {}", fields);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.error("{} = {}", entry.getKey(), entry.getValue());
        }
      }
      Iterator<String> it = fields.iterator();
      int index = 0;
      Object[] vals = new String[fields.size() + 1];
      while(it.hasNext()) {
        String field = it.next();
        vals[index++] = values.get(field).toString();
      }
      vals[index] = key;
      BoundStatement boundStmt = stmt.bind(vals);
      ResultSet rs = session.execute(boundStmt);
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();

      // Add fields
      StringBuilder fieldsString = new StringBuilder();
      StringBuilder placeHolders = new StringBuilder("?");
      for (int i = 0; i < fields.size(); i++) {
        fieldsString.append(", ").append("field").append(i);
        placeHolders.append(", ?");
      }
      SimpleStatement ss = SimpleStatement.newInstance(
        String.format("INSERT INTO " + table + " (y_id" + fieldsString + ") VALUES (" + placeHolders + ")")
      );
      ss.setConsistencyLevel(writeConsistency);
      PreparedStatement ps = session.prepare(ss);
      PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet<>(fields), ps);
      if (prevStmt != null) {
        ps = prevStmt;
      }
      if (logger.isDebugEnabled()) {
        logger.debug(ps.getQuery());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }
      BoundStatement boundStmt = ps.bind().setString(0, key);
      for (int i = 0; i < fields.size(); i++) {
        boundStmt.setString(i + 1, values.get("field" + i).toString());
      }
      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
    }
    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    try {
      PreparedStatement stmt = deleteStmt.get();

      // Prepare statement on demand
      if (stmt == null) {
        Delete query = deleteFrom(table).whereColumn(YCSB_KEY).isEqualTo(bindMarker());
        SimpleStatement ss = query.build();
        ss.setConsistencyLevel(writeConsistency);
        stmt = session.prepare(ss);
        boolean b = deleteStmt.compareAndSet(null, stmt);
        if (!b) {
          stmt = deleteStmt.get();
        }
      }
      ResultSet rs = session.execute(stmt.bind(key));
      // Should be only 1 row
      Row row = rs.one();
      if (row == null) {
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
      return Status.ERROR;
    }
  }
}
