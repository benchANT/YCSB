package site.ycsb.db;

import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.benchant.realtimeiot.QueryRegistry;
import com.benchant.realtimeiot.RtIotDb;
import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.NumericByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

public class TigerDataClient extends RtIotDb {

  private final TigerDataConfig config = new TigerDataConfig();
  private ConcurrentMap<String, PreparedStatement> cachedInsertStatements = new ConcurrentHashMap<String, PreparedStatement>();
  private ConcurrentMap<String, PreparedStatement> cachedQueryStatements = new ConcurrentHashMap<String, PreparedStatement>();

  private static String explainPrefix;
  private static boolean schemaInitialized = false;
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

  private synchronized static void initDbStructure(TigerDataConfig config) throws DBException {
    config.initialized = true;
    if(TigerDataClient.schemaInitialized) {
      return;
    }
    if(config.createSchema == true) {
      InitHelper.createSchema(config.connection);
    }
    if(config.refreshViews == true) {
      InitHelper.refreshViews(config);
      // try { Thread.sleep(10000); }
      // catch(InterruptedException ex){ Thread.currentThread().interrupt();}
    }
    TigerDataClient.schemaInitialized = true;
  }

  @Override
  public void init() throws DBException {
    if (config.initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    
    try {
        config.init(getProperties());
        explainPrefix = InitHelper.initExplainMode(getProperties());
        initDbStructure(config);
        initInsertPreparedStatements();
    }
    /*
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
  }

  private void initInsertPreparedStatements() throws SQLException {
    if (!cachedInsertStatements.containsKey(InsertHelper.OPERATOR_KEY)) {
      PreparedStatement operatorInsertStatement = config.connection.prepareStatement(InsertHelper.OPERATOR_INSERT_STRING);
      cachedInsertStatements.put(InsertHelper.OPERATOR_KEY, operatorInsertStatement);
    }
    if (!cachedInsertStatements.containsKey(InsertHelper.SITES_KEY)) {
      PreparedStatement siteInsertStatement = config.connection.prepareStatement(InsertHelper.SITES_INSERT_STRING);
      cachedInsertStatements.put(InsertHelper.SITES_KEY, siteInsertStatement);
    }
    if (!cachedInsertStatements.containsKey(InsertHelper.MACHINES_KEY)) {
      PreparedStatement machineInsertStatement = config.connection.prepareStatement(InsertHelper.MACHINES_INSERT_STRING);
      cachedInsertStatements.put(InsertHelper.MACHINES_KEY, machineInsertStatement);
    }

    if (!cachedInsertStatements.containsKey(InsertHelper.TELEMETRY_KEY)) {
      PreparedStatement telemetryInsertStatement = config.connection.prepareStatement(InsertHelper.TELEMETRY_INSERT_STRING);
      cachedInsertStatements.put(InsertHelper.TELEMETRY_KEY, telemetryInsertStatement);
    }
  }

  @Override
  public void cleanup() throws DBException {
    config.cleanup();
  }

    public TigerDataClient() {
        super();
    }

    @Override
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
            if (!config.autoCommit && config.batchSize <= 0) {
                config.connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error in inserting operator data: " + e);
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
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
            if (!config.autoCommit && config.batchSize <= 0) {
                config.connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error in inserting site data: " + e);
            return Status.ERROR;
        }
        return Status.OK;
    }
    
    @Override
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
            if (!config.autoCommit && config.batchSize <= 0) {
                config.connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error in inserting machine data: " + e);
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
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
            if (!config.autoCommit && config.batchSize <= 0) {
                config. connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error in inserting sensor data: " + e);
            return Status.ERROR;
        }
        return Status.OK;
    }

    private PreparedStatement createAndCacheReadStatement(String query) throws SQLException {
        PreparedStatement readStatement = config.connection.prepareStatement(query);
        PreparedStatement stmt = cachedQueryStatements.putIfAbsent(query, readStatement);
        if (stmt == null) {
            return readStatement;
        }
        return stmt;
    }

    @Override
    public Status query(String queryId, java.util.Map<String, Object> params, List<java.util.HashMap<String, site.ycsb.ByteIterator>> result) {
        String rawQuery = QueryRegistry.INSTANCE.getQuery(queryId);
        if(rawQuery == null) {
            System.err.println("query " + queryId + " not found");
            return Status.UNEXPECTED_STATE;
        }
        String[] patched = QueryFiller.patchQueryString(queryId, rawQuery, params);
        String cacheQuery = explainPrefix + patched[1];
        PreparedStatement readStatement = cachedQueryStatements.get(cacheQuery);
        try {
            if (readStatement == null) {
                readStatement = createAndCacheReadStatement(cacheQuery);
            }
            QueryFiller.fillQuery(queryId, readStatement, params);
            if(config.doDebug) {
              System.err.println("running query: " + readStatement.toString());
            }
        } catch(SQLException ex ) {
          if(config.doDebug) {
            System.err.println("could not initialize query for " + cacheQuery);
            ex.printStackTrace(System.err);
          }
          return Status.ERROR;
        }
        try(ResultSet resultSet = readStatement.executeQuery()){
            if (!resultSet.next()) {
                resultSet.close();
                return Status.NOT_FOUND;
            }
            ResultSetMetaData rsmd = resultSet.getMetaData();
            while(true) {
              HashMap<String,ByteIterator> row = new HashMap<>();
              for(int i = 1; i <= rsmd.getColumnCount(); i++) {
                int intType = rsmd.getColumnType(i);
                String label = rsmd.getColumnLabel(i);
                if(intType == JDBCType.BIGINT.getVendorTypeNumber().intValue()) {
                  long data = resultSet.getLong(i);
                  row.put(label, new NumericByteIterator(data));
                } else if(intType == JDBCType.DECIMAL.getVendorTypeNumber().intValue()){
                  double data = resultSet.getDouble(i);
                  row.put(label, new NumericByteIterator(data));
                } else if(intType == JDBCType.VARCHAR.getVendorTypeNumber().intValue() || 
                  intType == JDBCType.CHAR.getVendorTypeNumber().intValue()){
                  String data = resultSet.getString(i);
                  row.put(label, new StringByteIterator(data));
                } else if(intType == JDBCType.DOUBLE.getVendorTypeNumber().intValue()){
                  double doub = resultSet.getDouble(i);
                  row.put(label, new NumericByteIterator(doub));
		        } else if(intType == JDBCType.INTEGER.getVendorTypeNumber().intValue()) {
                  long data = resultSet.getLong(i);
                  row.put(label, new NumericByteIterator(data));		  
		        } else if(intType == JDBCType.NUMERIC.getVendorTypeNumber().intValue()) {
                  long data = resultSet.getLong(i);
                  row.put(label, new NumericByteIterator(data));		  
                } else if(intType == JDBCType.TIMESTAMP.getVendorTypeNumber().intValue()) {
                  Timestamp data = resultSet.getTimestamp(i);
                  row.put(label, new StringByteIterator(data.toString()));		  
                } else if(intType == JDBCType.DATE.getVendorTypeNumber().intValue()) {
                  Date data = resultSet.getDate(i);
                  row.put(label, new StringByteIterator(data.toString()));		  
                } else if (intType == JDBCType.OTHER.getVendorTypeNumber().intValue()) {
                    //  && "state_agg".equalsIgnoreCase(rsmd.getColumnTypeName(i))
                  // special handling for state_agg type which is returned as OTHER by the driver, but we know it is actually a long
                  Object data = resultSet.getObject(i);
                  // if(data.getClass().getName().equals("org.postgresql.util.PGInterval")) {
                  if("interval".equals(rsmd.getColumnTypeName(i))) {
                    row.put(label, new StringByteIterator(data.toString()));
                  } else {
                    throw new IllegalStateException("unknown OTHER type: " + intType + " / " + rsmd.getColumnTypeName(i));
                  }
                } else {
                    throw new IllegalStateException("unknown type: " + intType + " / " + rsmd.getColumnTypeName(i));
                }
              }
              result.add(row);
              if(!resultSet.next()) {
                break;
              }
            } 
        } catch(SQLException ex ) {
          if(config.doDebug) {
            System.err.println("sqlexception with query " + queryId + ": " + ex.getErrorCode());
            ex.printStackTrace(System.err);
          }
          return Status.ERROR;
        }
        if(config.doDebug) {
          System.err.println("result: " + result);
        }
        return Status.OK;
    }
}
