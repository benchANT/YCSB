package site.ycsb.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.benchant.realtimeiot.RIotQueryWorkload;

import site.ycsb.DBException;
import site.ycsb.Utils;

public final class InitHelper {
    
    public static String SCHEMA_FILE_NAME = "schema";
    public static String REFRESH_FILE_NAME = "refresh_views.sql";

    static void refreshViews(TigerDataConfig config) throws DBException {
        // Implementation for refreshing views
        Map<String,String> replacements = new HashMap<>();
        replacements.put("START_DAY", RIotQueryWorkload.getStartInstant().minus(1, java.time.temporal.ChronoUnit.DAYS).toString());
        replacements.put("END_DAY", RIotQueryWorkload.getStopInstant().plus(1, java.time.temporal.ChronoUnit.DAYS).toString());
        runOperationsFromFilename(config.connection, REFRESH_FILE_NAME, replacements);
        System.err.println("view refresh triggered and successfully executed");
    }

    static void createSchema(Connection connection) throws DBException {
        runOperationsFromFilename(connection, SCHEMA_FILE_NAME, Collections.emptyMap());
        System.err.println("schema creation triggered and successfully executed");
    }

    static void runOperationsFromFilename(Connection connection, String filename, Map<String, String> replacements) throws DBException {
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
        try {
            connection.setAutoCommit(true);
            // connection.setTransactionIsolation(1);
            for(String query: schemaDefinitions) {
                for(String key : replacements.keySet()) {
                    query = query.replaceAll(key, replacements.get(key));
                }
                System.err.println("Invoking query:\n" + query);
                Statement stmt = connection.createStatement();
                stmt.closeOnCompletion();
                boolean result = stmt.execute(query);
                stmt.close();
            }
        } catch(SQLException ex) {
            System.err.println(schemaDefinition);
            throw new DBException(ex);
        }
    }

    private InitHelper() {
        // no instances
    }
}
