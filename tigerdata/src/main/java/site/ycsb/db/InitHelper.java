package site.ycsb.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.benchant.realtimeiot.RIotQueryWorkload;

import site.ycsb.DBException;
import site.ycsb.Utils;

public final class InitHelper {
    
    public static String SCHEMA_FILE_NAME = "schema";
    public static String REFRESH_FILE_NAME = "refresh_views.sql";

    static void refreshViews(TigerDataConfig config) throws DBException {
        ExecutorService executor = Executors.newFixedThreadPool(40);
        List<Future<?>> futures = new ArrayList<>();

        Instant current = RIotQueryWorkload.getStartInstant().minus(1, ChronoUnit.DAYS);
        Instant stop = RIotQueryWorkload.getStopInstant().plus(1, ChronoUnit.DAYS);
        while(current.isBefore(stop)) {
            final Instant dayStart = current;
            final Instant dayEnd = current.plus(1, ChronoUnit.DAYS);
            futures.add(executor.submit(() -> {
                try (Connection connection = config.getTmpConnection()) {
                    System.err.println("refreshing views for day " + dayStart + " to " + dayEnd);
                    Map<String,String> replacements = new HashMap<>();
                    replacements.put("START_DAY", dayStart.toString());
                    replacements.put("END_DAY", dayEnd.toString());
                    runOperationsFromFilename(connection, REFRESH_FILE_NAME, replacements);
                } catch (DBException e) {
                    throw new RuntimeException(e);
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }));
            current = current.plus(1, ChronoUnit.DAYS);
            // System.err.println("refreshing views for day " + current.toString() + " to " + interval_end.toString());
            // Map<String,String> replacements = new HashMap<>();
            // replacements.put("START_DAY", current.toString());
            // replacements.put("END_DAY", interval_end.toString());
            // runOperationsFromFilename(config.connection, REFRESH_FILE_NAME, replacements);
            // current = interval_end;
            // interval_end = interval_end.plus(1, ChronoUnit.DAYS);
        }
        for(Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new DBException("View refresh failed", e);
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            System.err.println("view refresh triggered and successfully executed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DBException("View refresh interrupted", e);
        }
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
