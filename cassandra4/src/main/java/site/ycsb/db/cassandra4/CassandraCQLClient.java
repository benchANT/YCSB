/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
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
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package site.ycsb.db.cassandra4;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import site.ycsb.*;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

  public static final String MAX_CONNECTIONS_PROPERTY =
      "cassandra.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY =
      "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.connecttimeoutmillis";
  public static final String REQUEST_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.requesttimeoutmillis";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

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
        int fieldCount = Integer.parseInt(getProperties().getProperty("fieldcount"));
        String path = getProperties().getProperty("cassandra.path");
        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY,
            KEYSPACE_PROPERTY_DEFAULT);
        String maxConnections = getProperties().getProperty(
            MAX_CONNECTIONS_PROPERTY);
        String connectTimoutMillis = getProperties().getProperty(
            CONNECT_TIMEOUT_MILLIS_PROPERTY);
        String requestTimoutMillis = getProperties().getProperty(
            REQUEST_TIMEOUT_MILLIS_PROPERTY);
        //TODO: Check, if there is an equivalent for setCoreConnectionsPerHost
        ProgrammaticDriverConfigLoaderBuilder loader = DriverConfigLoader.programmaticBuilder();
        loader.withString(DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.QUORUM.name());
        if (connectTimoutMillis != null) {
          loader.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
              Duration.ofMillis(Integer.parseInt(connectTimoutMillis)));
        }
        if (maxConnections != null) {
          loader.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, Integer.parseInt(maxConnections));
        }
        if (requestTimoutMillis != null) {
          loader.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
              Duration.ofMillis(Integer.parseInt(requestTimoutMillis)));
        }

        session = CqlSession.builder()
            .withCloudSecureConnectBundle(Paths.get(path))
            .withAuthCredentials(username, password)
            .withKeyspace(keyspace)
            .withConfigLoader(loader.build())
            .build();

        session.getMetadata();
        Metadata metadata = session.getMetadata();
        logger.info("Connected to cluster: {}\n",
            metadata.getClusterName());
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

        if (fields == null) {
          String annQuery = String.format(
              "SELECT * FROM usertable WHERE y_id=?"
          );

          stmt = session.prepare(annQuery);
          ResultSet rs = session.execute(stmt.bind(key));
          // Should be only 1 row
          Row row = rs.one();
          ColumnDefinitions cd = row.getColumnDefinitions();

          for (ColumnDefinition def : cd) {
            ByteBuffer val = row.getBytesUnsafe(def.getName());
            if (val != null) {
              result.put(key, new ByteArrayByteIterator(val.array()));
            } else {
              result.put(key, null);
            }
          }
        } else {
          StringBuilder fieldsString = new StringBuilder();
          for (String col : fields) {
            fieldsString.append(col);
          }
          String annQuery = String.format(
              "SELECT " + fieldsString + " FROM usertable WHERE y_id=?"
          );
          stmt = session.prepare(annQuery);
          ResultSet rs = session.execute(stmt.bind(key));
          // Should be only 1 row
          Row row = rs.one();
          ColumnDefinitions cd = row.getColumnDefinitions();

          for (ColumnDefinition def : cd) {
            ByteBuffer val = row.getBytesUnsafe(def.getName());
            if (val != null) {
              result.put(key, new ByteArrayByteIterator(val.array()));
            } else {
              result.put(key, null);
            }
          }
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
      PreparedStatement stmt = updateStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        // Add fields
        StringBuilder fieldsString = new StringBuilder();
        for (String field : fields) {
          fieldsString.append(field).append("=?");
          fieldsString.append(", ");
        }
        PreparedStatement ps = session.prepare(String.format(
            "UPDATE " + table + " SET " + fieldsString.substring(0, fieldsString.length() - 2) + " WHERE y_id=?")
        );
        BoundStatement boundStmt = ps.bind().setString(1, key);
        ColumnDefinitions vars = ps.getVariableDefinitions();
        for (int i = 0; i < vars.size() - 1; i++) {
          String tmp = vars.get(i).toString().replace("ycsb.usertable.", "").replace(" TEXT", "");
          boundStmt.setString(i, values.get(tmp).toString());
        }
        session.execute(boundStmt);

        PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet(fields), ps);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQuery());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

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
      PreparedStatement ps = session.prepare(String.format(
          "INSERT INTO " + table + " (y_id" + fieldsString + ") VALUES (" + placeHolders + ")")
      );
      PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet(fields), ps);
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
    return Status.NOT_IMPLEMENTED;
  }

}
