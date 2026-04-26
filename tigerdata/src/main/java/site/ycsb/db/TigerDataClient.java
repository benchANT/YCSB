package site.ycsb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.benchant.realtimeiot.RtIotDb;
import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;

import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.util.PropertyParser;

public class TigerDataClient extends RtIotDb {

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /** The batch size for batched inserts. Set to >0 to use batching */
  public static final String DB_BATCH_SIZE = "db.batchsize";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  private Connection connection;
  private boolean initialized = false;
  private Properties props;
  private int jdbcFetchSize;
  private int batchSize;
  private boolean autoCommit;
  private boolean batchUpdates;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<String, PreparedStatement> cachedInsertStatements = new ConcurrentHashMap<String, PreparedStatement>();
  private long numRowsInOperatorsBatch = 0;
  private long numRowsInSitesBatch = 0;
  private long numRowsInMachinesBatch = 0;
  /**
   * Ordered field information for insert and update statements.

  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    String getFieldKeys() {
      return fieldKeys;
    }

    List<String> getFieldValues() {
      return fieldValues;
    }
  }
  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      if (!autoCommit) {
        conn.commit();
      }
      conn.close();
    }
  }

  Returns parsed int value from the properties if set, otherwise returns -1. * /
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. * /
  */

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String url = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    this.jdbcFetchSize = PropertyParser.parseIntProperty(props, JDBC_FETCH_SIZE, -1);
    this.batchSize = PropertyParser.parseIntProperty(props, DB_BATCH_SIZE, -1);

    this.autoCommit = PropertyParser.parseBooleanProperty(props, JDBC_AUTO_COMMIT, true);
    this.batchUpdates = PropertyParser.parseBooleanProperty(props, JDBC_BATCH_UPDATES, false);

    try {
//  The SQL Syntax for Scan depends on the DB engine
//  - SQL:2008 standard: FETCH FIRST n ROWS after the ORDER BY
//  - SQL Server before 2012: TOP n after the SELECT
//  - others (MySQL,MariaDB, PostgreSQL before 8.4)
//  TODO: check product name and version rather than driver name
    /*
      if (driver != null) {
        if (driver.contains("sqlserver")) {
          sqlserverScans = true;
          sqlansiScans = false;
        }
        if (driver.contains("oracle")) {
          sqlserverScans = false;
          sqlansiScans = true;
        }
        if (driver.contains("postgres")) {
          sqlserverScans = false;
          sqlansiScans = true;
        }
        Class.forName(driver);
      }
      */
      connection = DriverManager.getConnection(url, user, passwd);
      connection.setAutoCommit(autoCommit);
      // for a longer explanation see the README.md
      // semicolons aren't present in JDBC urls, so we use them to delimit
      // multiple JDBC connections to shard across.
      initInsertPreparedStatements();
    } /* catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } */catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }

    initialized = true;
  }

  private void initInsertPreparedStatements() throws SQLException {
    if (!cachedInsertStatements.containsKey(InsertHelper.OPERATOR_KEY)) {
      PreparedStatement operatorInsertStatement = connection.prepareStatement(InsertHelper.OPERATOR_INSERT_STRING);
      cachedInsertStatements.put(InsertHelper.OPERATOR_KEY, operatorInsertStatement);
    }
    if (!cachedInsertStatements.containsKey(InsertHelper.SITES_KEY)) {
      PreparedStatement siteInsertStatement = connection.prepareStatement(InsertHelper.SITES_INSERT_STRING);
      cachedInsertStatements.put(InsertHelper.SITES_KEY, siteInsertStatement);
    }
    if (!cachedInsertStatements.containsKey(InsertHelper.MACHINES_KEY)) {
      PreparedStatement machineInsertStatement = connection.prepareStatement(InsertHelper.MACHINES_INSERT_STRING);
      cachedInsertStatements.put(InsertHelper.MACHINES_KEY, machineInsertStatement);
    }

    if (!cachedInsertStatements.containsKey(InsertHelper.TELEMETRY_KEY)) {
      PreparedStatement telemetryInsertStatement = connection.prepareStatement(InsertHelper.TELEMETRY_INSERT_STRING);
      cachedInsertStatements.put(InsertHelper.TELEMETRY_KEY, telemetryInsertStatement);
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (batchSize > 0) {
      try {
        PreparedStatement st = cachedInsertStatements.get(InsertHelper.OPERATOR_KEY);
        if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInOperatorsBatch % batchSize != 0)) {
            st.executeBatch();
        }
        // commit un-finished batches
        /* FIXME
        for (PreparedStatement st : cachedInsertStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }*/
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        throw new DBException(e);
      }
    }

    try {
      connection.close();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

    public TigerDataClient() {
        super();
    }

    public Status insertOperatorData(List<RtIotOperatorData> operatorData) {
        System.err.println("Inserting batch for operators with " + operatorData.size() + " entries.");
        System.err.println("\t" + operatorData);
        try {
            PreparedStatement operatorInsertStatement = cachedInsertStatements.get(InsertHelper.OPERATOR_KEY);
            for (RtIotOperatorData data : operatorData) {
                InsertHelper.fillOperatorInsertStatement(operatorInsertStatement, data);
                operatorInsertStatement.addBatch();
            }
            operatorInsertStatement.executeBatch();
            if (!autoCommit && batchSize <= 0) {
                connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error in inserting operator data: " + e);
            return Status.ERROR;
        }
        return Status.OK;
    }

    public Status insertSiteData(List<RtIotSitesData> siteData){
        System.err.println("Inserting batch for sites with " + siteData.size() + " entries.");
        System.err.println("\t" + siteData);
        try {
            PreparedStatement siteInsertStatement = cachedInsertStatements.get(InsertHelper.SITES_KEY);
            for (RtIotSitesData data : siteData) {
                InsertHelper.fillSiteInsertStatement(siteInsertStatement, data);
                siteInsertStatement.addBatch();
            }
            siteInsertStatement.executeBatch();
            if (!autoCommit && batchSize <= 0) {
                connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error in inserting site data: " + e);
            return Status.ERROR;
        }
        return Status.OK;
    }
    
    public Status insertMachineData(List<RtIotMachineData> machineData){
        System.err.println("Inserting batch for machines with " + machineData.size() + " entries.");
        System.err.println("\t" + machineData);
        try {
            PreparedStatement machineInsertStatement = cachedInsertStatements.get(InsertHelper.MACHINES_KEY);
            for (RtIotMachineData data : machineData) {
                InsertHelper.fillMachineInsertStatement(machineInsertStatement, data);
                machineInsertStatement.addBatch();
            }
            machineInsertStatement.executeBatch();
            if (!autoCommit && batchSize <= 0) {
                connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error in inserting machine data: " + e);
            return Status.ERROR;
        }
        return Status.OK;
    }

    public Status insertSensorData(List<RtIotSensorData> sensorData) {
        System.err.println("Inserting batch for sender with " + sensorData.size() + " entries.");
        // System.err.println("\t" + sensorData);
        try {
            PreparedStatement telemetryInsertStatement = cachedInsertStatements.get(InsertHelper.TELEMETRY_KEY);
            for (RtIotSensorData data : sensorData) {
                InsertHelper.fillTelemetryInsertStatement(telemetryInsertStatement, data);
                telemetryInsertStatement.addBatch();
            }
            telemetryInsertStatement.executeBatch();
            if (!autoCommit && batchSize <= 0) {
                connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error in inserting sensor data: " + e);
            return Status.ERROR;
        }
        return Status.OK;
    }

}
