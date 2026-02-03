package site.ycsb.db;

final class BigQueryHelper {
    private BigQueryHelper() {
        // no instances
    }
    
    private static String TABLE_PH = "<table>";
    private static String COLUMNS_PH = "<columns>";
    private static String VALUES_PH = "( <values> )";
    public static final String INSERT_QUERY = "INSERT INTO " + TABLE_PH + " ( " + COLUMNS_PH + " ) VALUES " + VALUES_PH;
    
    static String getInsertQuery(String fullTableName, String primaryKey, String[] fields) {
        return getInsertQuery(fullTableName, primaryKey, fields, 1);
    }

    static String getInsertQuery(String fullTableName, String primaryKeyName, String[] fields, int rows) {
        StringBuilder parameters = new StringBuilder();
        for(int i = 0; i < rows; i++) {
            parameters.append(i == 0 ? " ( ?" : ", ( ?");
            for(int j = 0; j < fields.length; j++) {
                parameters.append(" , ?");
            }
            parameters.append(") \n");
        }
        // TODO: add caching
        return INSERT_QUERY.replace(TABLE_PH, fullTableName)
            .replace(COLUMNS_PH, primaryKeyName + (fields.length > 0 ? (", " + String.join(",", fields)) : ""))
            .replace(VALUES_PH, parameters.toString());
    }

    private static final String PK_PH = "<primary_key>";
    public static final String DELETE_QUERY = "DELETE FROM " + TABLE_PH + " WHERE " + PK_PH + " = ? ";
    static String getDeleteQuery(String fullTableName, String primaryKeyName) {
        return DELETE_QUERY.replace(TABLE_PH, fullTableName).replace(PK_PH, primaryKeyName);
    }

    public static final String READ_QUERY = "SELECT " + COLUMNS_PH + " FROM " + TABLE_PH + " WHERE " + PK_PH + " = ? ";
    static String getReadQuery(String fullTableName, String primaryKeyName, String[] fields) {
        String fieldString = fields.length > 0 
            ? primaryKeyName + ", " + String.join(",", fields)
            : " * ";
        return READ_QUERY.replace(TABLE_PH, fullTableName)
            .replace(COLUMNS_PH, fieldString)
            .replace(PK_PH, primaryKeyName);
    }

    public static final String UPDATE_QUERY = "UPDATE " + TABLE_PH + " SET " + COLUMNS_PH + " WHERE " + PK_PH + " = ? ";
    static String getUpdateQuery(String fullTableName, String primaryKeyName, String[] fieldNames) {
        StringBuilder parameters = new StringBuilder();
        for(int i = 0; i < fieldNames.length; i++) {
            if(i != 0) {
                parameters.append(" , ");    
            }
            parameters.append(fieldNames[i]).append(" = ? ");
        }
        return UPDATE_QUERY.replace(TABLE_PH, fullTableName)
            .replace(COLUMNS_PH, parameters.toString())
            .replace(PK_PH, primaryKeyName);
    }
}