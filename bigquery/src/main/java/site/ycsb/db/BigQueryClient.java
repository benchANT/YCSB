package site.ycsb.db;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQueryOptions.Builder;
import com.google.cloud.bigquery.BigQuerySQLException;
import com.google.cloud.bigquery.Connection;
import com.google.cloud.bigquery.ConnectionSettings;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.JobCreationMode;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

public final class BigQueryClient extends DB {
    
	private static final String YCSB_KEY = "YCSB_KEY";
    private static final String BIGQUERY_PROJECT_ID_PROPERTY = "bigquery.projectId";
    private static final String BIGQUERY_DATASET_PROPERTY = "bigquery.dataset";
    private static final String BIGQUERY_LOCATION_PROPERTY = "bigquery.location";
    private static final String BIGQUERY_USE_STREAMING_INSERT = "bigquery.insertStream";
	private static final String BIGQUERY_AUTHFILE_PATH_PROPERTY = "bigquery.authfile";
	// batchsize property aligned with other bindings
    private static final String BIGQUERY_BATCHSIZE_PROPERTY = "db.batchsize";
    
    private static final String BIGQUERY_USE_CACHE_PROPERTY = "bigquery.usecache";  
    
	private static final AtomicInteger clientCount = new AtomicInteger(0);
    // private final Properties driverProps = new Properties();
    private static boolean debug;
    private static String projectId;
    private static String dataset;
    private static String location;

    // private static String sessionId = null;
    private static int batchsize;
    private static boolean useInsertStream = false;
    private static boolean useCache;
	private static BigQuery bigQueryClient;

	// every client should have its own connection
	private Connection connection;	
    private int batchCount = 0;
	private QueryJobConfiguration.Builder batchJobBuilder = createJobBuilder("SELECT 1");

    final Map<String,String> queryLabels = new HashMap<>();

	private BigQuery initAuthentication(Properties props) throws IOException {
		String authFile = props.getProperty(BIGQUERY_AUTHFILE_PATH_PROPERTY);
		if(authFile == null || authFile.isEmpty()) {
			if (debug) {
				System.out.println("Using default application credentials");
			}
		} else {
			if (debug) {
				System.out.println("Using credentials from file: " + authFile);
			}
		}
		Builder b = BigQueryOptions.newBuilder().setProjectId(projectId);
        if(location != null) {
            b.setLocation(location);
        }
		// Initialize client that will be used to send requests. 
		// This client only needs to be created
        // once, and can be reused for multiple requests.
		GoogleCredentials creds = GoogleCredentials.fromStream(new FileInputStream(authFile));
        b.setCredentials(creds);
		BigQueryOptions bqOptions = b.build();
		bqOptions.setDefaultJobCreationMode(JobCreationMode.JOB_CREATION_OPTIONAL);
		System.err.println("creating client object."); 
		System.err.println("\t setting JobCreationMode to " + JobCreationMode.JOB_CREATION_OPTIONAL);
		System.out.println("\t - location: " + location);
		System.out.println("\t - projectId: " + projectId);
		return bqOptions.getService();
	}

	private Connection initConnection() {
		ConnectionSettings settings = ConnectionSettings.newBuilder()
			.setUseQueryCache(useCache)
			.build();
		// TODO: create connection settings object
		Connection conn = bigQueryClient.createConnection(settings);
		return conn;
	}

	private void printConfig() {
    	System.out.println("BigQueryClient: Listing property names:");
		System.out.println("\t - debug: " + debug);
		System.out.println("\t - dataset: " + dataset);
		System.out.println("\t - useCache: " + useCache);
		System.out.println("\t - batchSize: " + batchsize);
		System.out.println("\t - useInsertStream: " + useInsertStream);
		if(batchsize == 0 && useInsertStream) {
			System.out.println("\t WARNING db.batchsize == 0 and useInsertStream is enabled. This is not a useful cofiguration.");
		}
	}

    @Override
    public void init() throws DBException {
		Properties props = getProperties();
    	synchronized(BigQueryClient.class) {
			clientCount.incrementAndGet();
    		if(bigQueryClient != null) {
				connection = initConnection();
    			return;
    		}
    		System.out.println("initializing BigQuery client");
    		System.out.println("loading properties");
    		debug = "true".equalsIgnoreCase(props.getProperty("debug", "false"));

    		// Loading properties
    		projectId = props.getProperty(BIGQUERY_PROJECT_ID_PROPERTY);
    		dataset = props.getProperty(BIGQUERY_DATASET_PROPERTY);
    		location = props.getProperty(BIGQUERY_LOCATION_PROPERTY);
    		batchsize = Integer.parseInt(props.getProperty(BIGQUERY_BATCHSIZE_PROPERTY, "0"));
    		useInsertStream = "true".equalsIgnoreCase(props.getProperty(BIGQUERY_USE_STREAMING_INSERT, "false"));
			useCache = Boolean.parseBoolean(props.getProperty(BIGQUERY_USE_CACHE_PROPERTY, "false"));

			if(dataset == null) {
            	throw new DBException("at least one required parameter is not set: dataset = " + dataset + "'");
                // , account = '" + account + "'");
				// user = '" + user +
                //  "', password = '" + (password == null ? password : "******") + "' database = '" +
            }

			printConfig();
    		
			try {
	    		// Connecting to the database
				bigQueryClient = initAuthentication(props);
				connection = initConnection();
			} catch(IOException ioe) {
				System.err.println("BigQuery client: initialization failed: " + ioe.getMessage());
				throw new DBException(ioe);
			}
			System.err.println("BigQuery client: initialization successful!");
    	}
    }
    
    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        super.cleanup();
		try {
			connection.close();
		} catch(BigQuerySQLException ex) {
			System.err.println("closing connection failed: " + ex.getMessage());
		}
		int count = clientCount.decrementAndGet();
		if(count == 0) {
			// there is no real way to shut down the client
			bigQueryClient = null;
		}
    }

	@Override
    public final Status insert(String table, String key, Map<String, ByteIterator> values) {
		if(batchsize > 0) {
			if(useInsertStream) {
				System.err.println("Inserting in batches with insert streams has not been implemented.");
				return Status.NOT_IMPLEMENTED;
				// return insertWithInsertStream(table, key, values);
			} 
    		return batchInsert(table, key, values);
    	}
		return singleItemInsert(table, key, values);
	}

	    @Override
    public final Status delete(String table, String key) {
    	String deleteQuery = BigQueryHelper.getDeleteQuery(getFullTableName(table), YCSB_KEY);	
		QueryJobConfiguration.Builder builder = createJobBuilder(deleteQuery);
		builder.addPositionalParameter(QueryParameterValue.string(key));
		TableResult results = sendJobViaQueryJob(builder.build());
		if(results != null) {
			long rows = results.getTotalRows();
			if(rows == 1) return Status.OK;
			if(rows < 1) return Status.NOT_FOUND;
			return Status.UNEXPECTED_STATE;
		} else {
			// returned when an exception was thrown
			return Status.ERROR;
		}
    }

    @Override
    public final Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result){
		String[] fieldsArray = fields == null ? new String[0] : fields.toArray(new String[fields.size()]);
    	String readQuery = BigQueryHelper.getReadQuery(getFullTableName(table), YCSB_KEY, fieldsArray);
		QueryJobConfiguration.Builder builder = createJobBuilder(readQuery);
		builder.addPositionalParameter(QueryParameterValue.string(key));
    	TableResult results = sendJobViaQueryJob(builder.build());
		if(results != null) {
			long rows = results.getTotalRows();
			if(rows == 1) return Status.OK;
			if(rows < 1) return Status.NOT_FOUND;
			return Status.UNEXPECTED_STATE;
		} else {
			// returned when an exception was thrown
			return Status.ERROR;
		}
    }
    
    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
		String[] fields = values.keySet().toArray(new String[values.size()]);
    	String updateQuery = BigQueryHelper.getUpdateQuery(getFullTableName(table), YCSB_KEY, fields);
		QueryJobConfiguration.Builder builder = createJobBuilder(updateQuery);
		appendRowToBuilder(builder, fields, values);
		builder.addPositionalParameter(QueryParameterValue.string(key));
		TableResult results = sendJobViaQueryJob(builder.build());
		if(results != null) {
			long rows = results.getTotalRows();
			if(rows == 1) return Status.OK;
			if(rows < 1) return Status.NOT_FOUND;
			return Status.UNEXPECTED_STATE;
		} else {
			// returned when an exception was thrown
			return Status.ERROR;
		}
    }

    @Override
    public final Status scan(String table, String startkey, int recordcount, Set<String> fields,
                                Vector<HashMap<String, ByteIterator>> result) {
    	return Status.NOT_IMPLEMENTED;
    }

	private final Status singleItemInsert(String table, String key, Map<String, ByteIterator> values) {
    	// Defining the parameters that need to be bound into the INSERT statement
        String fullTableName = getFullTableName(table);
        // Getting the fields with their values
        String[] fields = values.keySet().toArray(new String[values.size()]);
		String parametrizedQuery = BigQueryHelper.getInsertQuery(fullTableName, YCSB_KEY, fields);
		if(debug) {
			System.err.println("issuing parametrized query against BigQuery: " + parametrizedQuery);
		}
		
		QueryJobConfiguration.Builder queryConfigBuilder = createJobBuilder(parametrizedQuery);
		queryConfigBuilder.addPositionalParameter(QueryParameterValue.string(key));
		appendRowToBuilder(queryConfigBuilder, fields, values);
		// List<Parameter> params = new ArrayList<>(fields.length + 1);
		// params.add(0, Parameter.newBuilder().setValue(QueryParameterValue.string(key)).build()); 
		TableResult results = sendJobViaQueryJob(queryConfigBuilder.build());
		if(results != null) {
			long rows = results.getTotalRows();
			if(rows == 1) return Status.OK;
			if(debug) {
				System.err.println("insert single item resulted in unexpected number of affected rows: " + rows);
			}
			return Status.UNEXPECTED_STATE;
		}
		return Status.ERROR;
	}

	private TableResult sendJobViaQueryJob(QueryJobConfiguration job) {
		System.err.println("Sending job: " + job);
		try {
			// this does not seem to work. NPEs drain the thread pool and kill the driver
			// BigQueryResult result = connection.executeSelect(parametrizedQuery, params);
			// we are using Jobs instead.
			return bigQueryClient.query(job);
		} catch(BigQueryException | InterruptedException e) {
			if(debug) {
				System.err.println("An Error has occurred when sending a job: " + e.toString());
			}
		}
		return null;
	}

	private Status batchInsertSend(String fullTableName, String[] fields) {
		String parametrizedQuery = BigQueryHelper.getInsertQuery(fullTableName, YCSB_KEY, fields, batchsize);
		// we only set the query here as otherwise, we had to initilize it before building
		// the first batch, which makes finding the right table name way harder.
		batchJobBuilder.setQuery(parametrizedQuery);
		if(debug) {
			System.err.println("BigQueyClient: sending batch of " + batchsize + " elements ");
			System.err.println("BigQueyClient: " + batchJobBuilder);
		}

		TableResult results = sendJobViaQueryJob(batchJobBuilder.build());
		if(results != null) {
			long rows = results.getTotalRows();
			if(rows == batchsize) return Status.OK;
			if(debug) {
				System.err.println("item batch insert resulted in unexpected number of affected rows: " + rows);
			}
			return Status.UNEXPECTED_STATE;
		} 
		return Status.ERROR;
	}
    
    private Status batchInsert(String table, String key, Map<String, ByteIterator> values) {
		String[] fields = values.keySet().toArray(new String[values.size()]);
		batchJobBuilder.addPositionalParameter(QueryParameterValue.string(key));
		appendRowToBuilder(batchJobBuilder, fields, values);

		if(batchsize > ++batchCount) {
			return Status.BATCHED_OK;
		}
		// batch is full, we send the batch and re-initialize the query
		Status result = batchInsertSend(getFullTableName(table), fields);
		// re-initialize the query mechanism
		batchCount = 0;
		batchJobBuilder = createJobBuilder("SELECT 1");
		return result;
	}

	private static QueryJobConfiguration.Builder createJobBuilder(String parametrizedQuery) {
		return QueryJobConfiguration.newBuilder(parametrizedQuery)
				.setCreateSession(false)
				.setJobCreationMode(JobCreationMode.JOB_CREATION_OPTIONAL)
				.setUseQueryCache(false)
				.setParameterMode("POSITIONAL")
				.setUseLegacySql(false);
	}

	private final static String getFullTableName(String table) {
		return projectId + "." + dataset + "." + table;
	}

	private static void appendRowToBuilder(QueryJobConfiguration.Builder builder, String[] fieldNames, Map<String, ByteIterator> values) {
		// builder.addPositionalParameter(QueryParameterValue.string(key));
		for(int i = 0; i < fieldNames.length; i++) {
			String entry = values.get(fieldNames[i]).toString();
			// params.add(i + 1, Parameter.newBuilder().setValue(QueryParameterValue.string(entry)).build()); 
			builder.addPositionalParameter(QueryParameterValue.string(entry));
		}
	}
}

/*
	    private int[] sortQuery(String[] fields) {
    	
    	int[] sorting = new int[fields.length];
    	
    	if(columnNames == null) { // If there has been an error with determining the columns
    		for(int i = 0; i < sorting.length; i++) { // Use normal order
    			sorting[i] = i;
    		}
    		return sorting;
    	}
    	
    	for(int i = 0; i < sorting.length; i++) { // Determine custom ordering of insert colums
    		for(int j = 1; j < columnNames.length; j++) {
    			if(columnNames[j].equals(fields[i].toUpperCase())) {
    				sorting[i] = j - 1;
    				break;
    			}
    		}
    	}
    	return sorting;	
    }
	private final Status insertWithInsertStream(String table, String key, Map<String, ByteIterator> values) {
		// Very fast inserts, but UPDATE and DELETE might only be called up to 90 minutes later	
    	// Defining variables necessary for the InsertAllRequest
		TableId tableId = TableId.of(dataset, table);
		Map<String, String> rowContent = new HashMap<>();
		rowContent.put(YCSB_KEY, key);
		
		// Getting the fields with their values
		Set<String> fields = values.keySet();
		for(String field : fields) {
			// Formatting the value so that the string escape characters don't work anymore
			String val = values.get(field).toString();
			val = val.replace("\\", "\\\\");
			val = val.replace("'", "\\'");
			rowContent.put(field, val);
		}
        
		// FIXME: this method is called "insertWithInsertStream", but does not use an insert stream,
		// but InsertAllRequest. This is not the same
		// https://docs.cloud.google.com/bigquery/docs/streaming-data-into-bigquery
		InsertAllRequest insertRequest = InsertAllRequest.newBuilder(tableId).addRow(rowContent).build();
		// this is not how it is supposed to work. Here, we are using the whole bulk loading 
		// functionality for loading a single row to the database. Insert Streams and "InsertAll"
		// should only be used for buld loading
		try {
			InsertAllResponse response = bigQueryClient.insertAll(insertRequest);
			if(response.hasErrors()) {
				return Status.ERROR;
			}
		} catch(BigQueryException e) {
			return Status.ERROR;
		}
		
		return Status.OK;    		
	}
*/
	/*
    private static String computeQueryTag(Properties props) {
        int threadcount = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
        String queryId = "1";//TODO: REPAIR THIS -> props.getProperty(AnalyticsQueryWorkload.ANALYTICS_QUERY_NUMBER_PROPERTY);
        return queryId + "---" + threadcount + "threads";
    }

    private void initQueryMap() {
        String tag = computeQueryTag(getProperties());
        queryLabels.put("query", tag);
        queryLabels.put("thread", Thread.currentThread().getName().toLowerCase());
    }

			// Query the column names
			if(doDebug) {
				System.out.println("BigQuery client: columNames");
			}
			String tab = projectId + "." + dataset + ".usertable";
	    	String readQuery = "SELECT * FROM " + tab + " LIMIT 1";
	    	
	    	try {
	        	
	    		// Creating the statement
	    		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(readQuery).setUseLegacySql(false).build();
	    		TableResult res = bigquery.query(queryConfig);
	    		
	    		FieldList fl = res.getSchema().getFields();
	    		columnNames = new String[fl.size()];
	  
	    		for(int i = 0; i < columnNames.length; i++) {
	    			columnNames[i] = fl.get(i).getName().toUpperCase();
	    			if(doDebug) {
	    				System.out.println(columnNames[i]);
	    			}
	    		}
	    		
	    	} catch(BigQueryException | InterruptedException e) {
	    		System.err.println("An Error has occurred: " + e.toString());
	    		columnNames = null;
	    	}
	*/

	/*
    		try { // Execute the batch insert
    		
    			TableId tableId = TableId.of(dataset, table);
        		
        		WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
        		
        		String jobName = "jobId_" + UUID.randomUUID().toString();
        		JobId jobId = JobId.newBuilder().setLocation(location).setJob(jobName).build();
        		
        		try(TableDataWriteChannel writer = bigQueryClient.writer(jobId, writeChannelConfiguration);
        				OutputStream stream = Channels.newOutputStream(writer)) {
        			stream.write(batchBuffer.toString().getBytes());
        		} catch(IOException e) {
        			System.err.println("IOException: " + e.toString());
        			return Status.ERROR;
        		}
        		
        		Job job = bigQueryClient.getJob(jobId);
        		Job completedJob = job.waitFor();
        		if (completedJob != null && completedJob.getStatus().getError() != null){
        			System.err.println("Batch Insert failed with following error: " + job.getStatus().getError());
        			return Status.ERROR;
        		}
        		
        		current_batch_size = 0;
        		return Status.OK;
    			
    		} catch(BigQueryException e) {
    			System.err.println("An error has ocurred: " + e.toString());
    			return Status.ERROR;
    		} catch(InterruptedException e1) {
    			System.err.println("InterruptedException: " + e1.toString());
    			return Status.ERROR;
    		}
    		
    	}
		
    	return Status.BATCHED_OK;
    	
    }
 	*/
