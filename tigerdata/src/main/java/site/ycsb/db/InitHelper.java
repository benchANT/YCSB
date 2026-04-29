package site.ycsb.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import site.ycsb.DBException;
import site.ycsb.Utils;

public final class InitHelper {
    
    public static String SCHEMA_FILE_NAME = "schema";

    static void createSchema(Connection connection) throws DBException {
        String schemaDefinition = Utils.readByteStreamToString(
            ClassLoader.getSystemResourceAsStream(SCHEMA_FILE_NAME)
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
                Statement stmt = connection.createStatement();
                stmt.closeOnCompletion();
                boolean result = stmt.execute(query);
                stmt.close();
            }
            System.err.println("schema creation triggered and successfully executed");
        } catch(SQLException ex) {
            System.err.println(schemaDefinition);
            throw new DBException(ex);
        }
    }

    private InitHelper() {
        // no instances
    }
}
