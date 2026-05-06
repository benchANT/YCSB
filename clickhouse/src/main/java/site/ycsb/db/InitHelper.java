package site.ycsb.db;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;

import site.ycsb.DBException;
import site.ycsb.Utils;
import site.ycsb.util.PropertyParser;

public final class InitHelper {

    public static String SCHEMA_FILE_NAME = "schema";
    public static String REFRESH_FILE_NAME = "refresh_views.sql";

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

    static void runOperationsFromFilename(Client client, String filename) throws DBException {

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
            runOperation(client, query);
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

    static void refreshViews(Client client) throws DBException {
        runOperationsFromFilename(client, REFRESH_FILE_NAME);
    }
    
    static void createSchema(Client client) throws DBException {
        runOperationsFromFilename(client, SCHEMA_FILE_NAME);
    }

    private InitHelper() {
        // no instances
    }
}
