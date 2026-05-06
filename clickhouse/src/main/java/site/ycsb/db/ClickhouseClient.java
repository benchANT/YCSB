package site.ycsb.db;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.benchant.realtimeiot.QueryRegistry;
import com.benchant.realtimeiot.RtIotDb;
import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader.ByteBufferAllocator;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader.CachingByteBufferAllocator;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.NumericByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.InsertHelper.MachineHelper;
import site.ycsb.db.InsertHelper.OperatorHelper;
import site.ycsb.db.InsertHelper.SiteHelper;
import site.ycsb.db.InsertHelper.TelematicsHelper;
import site.ycsb.util.PropertyParser;

public final class ClickhouseClient extends RtIotDb {

    public static final String CLICKHOUSE_CREATE_SCHEMA = "clickhouse.createschema";
    public static final String CLICKHOUSE_REFRESH_VIEWS = "clickhouse.refreshviews";

    private static final String CLICKHOUSE_QUERY_TIMEOUT = "clickhouse.timeout";
    private static final String CLICKHOUSE_QUERY_TIMEOUT_DEFAULT = "3";
    private static final String CLICKHOUSE_MAX_CONNECTIONS = "clickhouse.maxConnections";
    private static final String CLICKHOUSE_MAX_CONNECTIONS_DEFAULT = "200";

    private static final String CLICKHOUSE_ENDPOINT = "clickhouse.endpoint";
    private static final String CLICKHOUSE_USER = "clickhouse.user";
    private static final String CLICKHOUSE_PASSWORD = "clickhouse.password";
    private static final String CLICKHOUSE_DATABASE = "clickhouse.database";

    public static String OPERATOR_TABLE_NAME = "operators";
    public static String SITE_TABLE_NAME = "sites";
    public static String MACHINE_TABLE_NAME = "machines";
    public static String TELEMATICS_TABLE_NAME = "telematics_data";
    // public static final String BATCH_SIZE = "db.batchsize";
    // private static final Map<String,List<Map<String,String>>> batchList = new HashMap<>();
    // private static final Map<String,String> tableNames = new HashMap<>();

    // for Java Clickhouse offers a regular JDBC driver. This, however, is based
    // on an outdated driver version (v1). Here, we build our binding directly
    // using version v2 of the driver 
    private static int batchsize;
    private static QuerySettings querySettings;
    private static int queryTimeoutSeconds;
    private static Client client;
    private static boolean doDebug = false;
    private static boolean createSchema = false;
    private final ByteBufferAllocator myAllocator = new CachingByteBufferAllocator();
    private int batchCounter = 0;

    protected void debug(String ... data) {
        if(doDebug) {
            System.err.println(String.join(" ", data));
        }
    }

    protected void debug(String message) {
        if(doDebug) {
            System.err.println(message);
        }
    }

    @Override
    public void init() throws DBException {
        synchronized(ClickhouseClient.class) {
            if(client != null) {
                return;
            }
            Properties props = getProperties();
            String endpoint = props.getProperty(CLICKHOUSE_ENDPOINT);
            String user = props.getProperty(CLICKHOUSE_USER);
            String password = props.getProperty(CLICKHOUSE_PASSWORD);
            String database = props.getProperty(CLICKHOUSE_DATABASE);
            String clients = props.getProperty(site.ycsb.Client.THREAD_COUNT_PROPERTY, "1");
            String maxConnectionsString = props.getProperty(CLICKHOUSE_MAX_CONNECTIONS, CLICKHOUSE_MAX_CONNECTIONS_DEFAULT);

            int maxConnections = Integer.parseInt(maxConnectionsString);
            queryTimeoutSeconds = Integer.parseInt(props.getProperty(CLICKHOUSE_QUERY_TIMEOUT, CLICKHOUSE_QUERY_TIMEOUT_DEFAULT));
            // batchsize = Integer.parseInt(props.getProperty(BATCH_SIZE, "1"));
            querySettings = new QuerySettings()
                .logComment("{ \"clients\": \"" + clients + "\" }")
                .setMaxExecutionTime(queryTimeoutSeconds);
            client = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .enableConnectionPool(true)
                .setMaxConnections(maxConnections)
                .setMaxRetries(0)
                .useAsyncRequests(false)
                .setDefaultDatabase(database)
                .setExecutionTimeout(queryTimeoutSeconds, ChronoUnit.SECONDS)
                .setSocketTimeout((queryTimeoutSeconds + 2) * 1000)
                .build();
            
            InitHelper.helpWithInit(client, props, database);

            registerSchemaMapper(OPERATOR_TABLE_NAME, OperatorHelper.class);
            registerSchemaMapper(SITE_TABLE_NAME, SiteHelper.class);
            registerSchemaMapper(MACHINE_TABLE_NAME, MachineHelper.class);
            registerSchemaMapper(TELEMATICS_TABLE_NAME, TelematicsHelper.class);
            doDebug = Boolean.parseBoolean(props.getProperty("debug", "false"));
            printClickhouseConfig(maxConnections);
        }
    }

    private void registerSchemaMapper(String tableName, Class<?> mapperClass) {
        TableSchema schema = client.getTableSchema(tableName);
        // System.err.println("Registering class " + mapperClass +  " for table name '" + tableName + "' using TableSchema " + schema);
        client.register(mapperClass, schema);
    }

    private void printClickhouseConfig(int maxConnections) {
        System.err.println("using timeout: " + queryTimeoutSeconds);
        System.err.println("using connection pool with " + maxConnections + " max connections");
        System.err.println("using debug: " + doDebug);
        System.err.println("using client settings: " + client.getConfiguration());
        System.err.println("using batch size: " + batchsize);
    }

    private Status insertAny(String tableName, List<?> toSend) {
        try {
            CompletableFuture<InsertResponse> f = client.insert(tableName, toSend);
            InsertResponse resp = f.get();
            if(resp.getWrittenRows() != toSend.size()) {
                return Status.UNEXPECTED_STATE;
            }
            return Status.OK;
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
            return Status.ERROR;
        }
    }

    @Override
    public Status insertOperatorData(List<RtIotOperatorData> operatorData) {
        return insertAny(OPERATOR_TABLE_NAME, OperatorHelper.wrap(operatorData));
    }

    @Override
    public Status insertSiteData(List<RtIotSitesData> siteData){
        return insertAny(SITE_TABLE_NAME, SiteHelper.wrap(siteData));
    }

    @Override
    public Status insertMachineData(List<RtIotMachineData> machineData){
        return insertAny(MACHINE_TABLE_NAME, MachineHelper.wrap(machineData));
    }

    @Override
    public Status insertSensorData(List<RtIotSensorData> sensorData) {
        return insertAny(TELEMATICS_TABLE_NAME, TelematicsHelper.wrap(sensorData));
    }

    @Override
    public Status query(String queryId, java.util.Map<String, Object> params, List<java.util.HashMap<String, site.ycsb.ByteIterator>> result) {
        String rawQuery = QueryRegistry.INSTANCE.getQuery(queryId);
        if(rawQuery == null) {
            System.err.println("query '" + queryId + "'' not found");
            return Status.UNEXPECTED_STATE;
        }
        String[] patched = QueryFiller.patchQueryString(queryId, rawQuery, params);
        String nextQuery = patched[1];
        if(doDebug) {
            System.err.println("\nrunning query '" + queryId + "' with parameters " + params);
            System.err.println("\n\t '" + nextQuery);
        }
        QueryResponse response = null;
        ClickHouseBinaryFormatReader reader = null;
        // we cannot use auto-close as close throws an exception
        try {
            response = client.query(nextQuery, params, querySettings).get(queryTimeoutSeconds, TimeUnit.SECONDS);
            if(response.getResultRows() == 0) {
                debug("\n\tresults\n\t\tnot found", queryId, "(", rawQuery, ")");
                return Status.NOT_FOUND;
            }
            // Create a reader to access the data in a convenient way
            reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream(), response.getSettings(), myAllocator);
            parseResults(reader, result);
        } catch(InterruptedException | TimeoutException | ExecutionException ex) {
            debug("Exception occurred: " + ex.getMessage() + " (" + ex.getClass() + ")" );
            return Status.ERROR;
        } catch(com.clickhouse.client.api.ServerException ex){
            debug("Exception occurred: " + ex.getMessage() + " (" + ex.getClass() + ")" );
            ex.printStackTrace();
            return Status.ERROR;
        } catch(com.clickhouse.client.api.ClientException ex) {
            debug("Exception occurred: " + ex.getMessage() + " (" + ex.getClass() + ")" );
            if(doDebug) {
                ex.printStackTrace(System.err);
            }
            return Status.ERROR;            
        } finally {
            if(reader != null) {
                try { reader.close(); }
                catch(Exception ex) {
                    debug("Exception occurred at reader close: " + ex.getMessage());
                }
            }
            if(response != null) {
                try { response.close(); }
                catch(Exception ex) {
                    debug("Exception occurred at response close: " + ex.getMessage());
                }
            }
        }
        return Status.OK;
    }

    private void parseResults(ClickHouseBinaryFormatReader reader, List<java.util.HashMap<String, site.ycsb.ByteIterator>> result) {
        TableSchema schema = reader.getSchema();
        Map<String,ClickHouseDataType> columns = new HashMap<>();
        for(ClickHouseColumn c : schema.getColumns()) {
            columns.put(c.getColumnName(), c.getDataType());
        }
        while (reader.hasNext()) {
            Map<String, Object> line = reader.next(); // Read the next record from stream and parse it
            HashMap<String,ByteIterator> bit = new HashMap<String,ByteIterator>();
            for(String s : line.keySet()) {
                ClickHouseDataType type = columns.get(s);
                switch (type) {
                    case Int64:
                        long ll = reader.getLong(s);
                        bit.put(s, new NumericByteIterator(ll));
                        break;
                    case UInt16:
                    case Int16:
                    case Int32:
                        int i = reader.getInteger(s);
                        bit.put(s, new NumericByteIterator(i));
                        break;
                    case UInt32:
                    case UInt64:
                        long l = reader.getBigInteger(s).longValue();
                        bit.put(s, new NumericByteIterator(l));
                        break;
                    case Date:
                    case Date32:
                        // Instant i4 = reader.getInstant(s);
                        ZonedDateTime zdt = reader.getZonedDateTime(s);
                        bit.put(s, new StringByteIterator(zdt.toString()));
                        break;
                    case DateTime32:
                        Instant i2 = reader.getInstant(s);
                        bit.put(s, new StringByteIterator(i2.toString()));
                        break;
                    case DateTime:
                    case DateTime64:
                        LocalDateTime i3 = reader.getLocalDateTime(s);
                        bit.put(s, new StringByteIterator(i3.toString()));
                        break;
                    case Float32:
                        float f = reader.getFloat(s);
                        bit.put(s, new NumericByteIterator(f));
                        break;
                    case Float64:
                        double d = reader.getDouble(s);
                        bit.put(s, new NumericByteIterator(d));
                        break;
                    case String:
                        String ss = reader.getString(s);
                        bit.put(s, new StringByteIterator(ss));
                        break;
                    case Decimal:
                        String dec = reader.getString(s);
                        bit.put(s, new StringByteIterator(dec));
                        break;
                    default:
                        throw new UnsupportedOperationException("no support for data type " +  type + " available");
                }
            }
            result.add(bit);
        }
        debug("\tresults");
        result.forEach(row -> debug("\t\t data: " + row.toString()));
    }

    /*
    public final Status query(String table, String query, Map<String, Object> params, List<HashMap<String, ByteIterator>> result) {
        
        
        try {

            

        
    }
 */
}
