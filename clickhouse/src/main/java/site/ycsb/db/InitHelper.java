package site.ycsb.db;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import com.benchant.realtimeiot.RIotQueryWorkload;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;

import site.ycsb.DBException;
import site.ycsb.Utils;
import site.ycsb.util.PropertyParser;

public final class InitHelper {

    public static final String SCHEMA_FILE_NAME = "schema";
    public static final String REFRESH_FILE_NAME = "refresh_views.sql";
    public static final String FLEET_METRICS_FILE_NAME = "refresh_fleet_metrics.sql";
    public static final String REFRESH_OPERATOR_ACTIVITY_FILE_NAME = "refresh_operator_activity.sql";
    public static final String REFRESH_DAILY_MACHINE_USAGE_FILE_NAME = "refresh_daily_machine_usage.sql";
    public static final String CLICKHOUSE_EXPLAIN_MODE = "clickhouse.explainmode";

    static void helpWithInit(Client client, Properties props, String databaseName) throws DBException {

        boolean createSchema = PropertyParser.parseBooleanProperty(props, ClickhouseClient.CLICKHOUSE_CREATE_SCHEMA, false);
        boolean refreshViews = PropertyParser.parseBooleanProperty(props, ClickhouseClient.CLICKHOUSE_REFRESH_VIEWS, false);
        if(createSchema == true) {
            runOperationWithDefaultDatabase(client, "CREATE DATABASE IF NOT EXISTS " + databaseName + ";");
            InitHelper.createSchema(client);
        }
        if(refreshViews == true) {
            InitHelper.refreshViews(client);
            try { Thread.sleep(10000); }
            catch(InterruptedException ex){ Thread.currentThread().interrupt();}
        }
    }

    static String initExplainMode(Properties props) {
        String explainMode = props.getProperty(CLICKHOUSE_EXPLAIN_MODE, "none");
        switch(explainMode.toLowerCase()) {
            case "none":
                return "";
            case "tree":
                return "EXPLAIN QUERY TREE ";
            case "ast":
                return "EXPLAIN AST ";
            case "syntax":
                return "EXPLAIN SYNTAX ";
            case "plan":
                return "EXPLAIN PLAN ";
            case "pipeline":
                return "EXPLAIN PIPELINE ";
            case "estimate":
                return "EXPLAIN ESTIMATE ";
            default:
                System.err.println("Unknown explain mode: " + explainMode + ". Defaulting to 'none'.");
                return "";
        }
    }

    static void runOperationWithDefaultDatabase(Client client, String query) throws DBException {
        System.err.println("sending schema request: " + query);
        try {
            CommandSettings settings = new CommandSettings();
            settings.setDatabase("default");
            CompletableFuture<CommandResponse> future = client.execute(query, settings);
            CommandResponse response = future.get();
        } catch(Exception ex) {
            throw new DBException(ex);
        }
    }

    static void runOperation(Client client, String query) throws DBException {
        System.err.println("sending schema request: " + query);
        try {
            CompletableFuture<CommandResponse> future = client.execute(query);
            CommandResponse response = future.get();
        } catch(Exception ex) {
            throw new DBException(ex);
        }
    }

    static void runOperationsFromFilename(Client client, String filename, Map<String, String> replacements) throws DBException {

        String schemaDefinition = Utils.readByteStreamToString(
            ClassLoader.getSystemResourceAsStream(filename)
        );
        if(schemaDefinition == null) {
            throw new DBException("could not initilize schema. Schema not found.");
        }
        String[] schemaDefinitions = schemaDefinition.split("\\R\\s*\\R", 0);
        
        System.err.println("=============================");
        System.err.println("Got schema definitions:\n\n");
        System.err.println(String.join("\n>=====<\n", schemaDefinitions));
        System.err.println("=============================");
        for(String query: schemaDefinitions) {
            String sendQuery = query;
            for(String key : replacements.keySet()) {
                sendQuery = sendQuery.replaceAll(key, replacements.get(key));
            }
            runOperation(client, sendQuery);
        }
        /*
        connection.setAutoCommit(true);
        // connection.setTransactionIsolation(1);
        for(String query: schemaDefinitions) {
            Statement stmt = connection.createStatement();
            stmt.closeOnCompletion();
            boolean result = stmt.execute(query);
            stmt.close();
        }
        */
    }
    private static String formatInstant(Instant instant) {
        return instant.toString()
            .replace("T", " ")
            .replace("Z", "")
                + "."  + 
                instant.get(ChronoField.MICRO_OF_SECOND);
    }

    static void refreshViews(Client client) throws DBException {
        Instant current = RIotQueryWorkload.getStartInstant().minus(1, ChronoUnit.DAYS);
        Instant stop = RIotQueryWorkload.getStopInstant().plus(1, ChronoUnit.DAYS);
        while(current.isBefore(stop)) {
            final Instant dayStart = current;
            final Instant dayEnd = current.plus(1, ChronoUnit.DAYS);
            System.err.println("refreshing views for day " + dayStart + " to " + dayEnd);
            Map<String,String> replacements = new HashMap<>();
            replacements.put("START_DAY", formatInstant(dayStart));
            replacements.put("END_DAY", formatInstant(dayEnd));
            runOperationsFromFilename(client, FLEET_METRICS_FILE_NAME, replacements);
            runOperationsFromFilename(client, REFRESH_OPERATOR_ACTIVITY_FILE_NAME, replacements);
            runOperationsFromFilename(client, REFRESH_DAILY_MACHINE_USAGE_FILE_NAME, replacements);
            current = current.plus(1, ChronoUnit.DAYS);
            // System.err.println("refreshing views for day " + current.toString() + " to " + interval_end.toString());
            // Map<String,String> replacements = new HashMap<>();
            // replacements.put("START_DAY", current.toString());
            // replacements.put("END_DAY", interval_end.toString());
            // runOperationsFromFilename(config.connection, REFRESH_FILE_NAME, replacements);
            // current = interval_end;
            // interval_end = interval_end.plus(1, ChronoUnit.DAYS);
        }
    }
    
    static void createSchema(Client client) throws DBException {
        runOperationsFromFilename(client, SCHEMA_FILE_NAME, Collections.emptyMap());
    }

    private InitHelper() {
        // no instances
    }
}
