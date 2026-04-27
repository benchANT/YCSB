package site.ycsb.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;

import com.benchant.realtimeiot.RIotParameterConstants;

public final class QueryFiller {
    
    static String[] patchQueryString(String queryId, String rawQuery, java.util.Map<String, Object> params) {
        switch (queryId) {
            case "query01":
                rawQuery = patchQuery01(rawQuery, params);
                break;
            case "query06":
                rawQuery = patchQuery06(rawQuery, params);
                break;
            case "query14":
                rawQuery = patchQuery14(rawQuery, params);
                break;
            case "query15":
                rawQuery = patchQuery15(rawQuery, params);
                break;
            case "query16":
                rawQuery = patchQuery16(rawQuery, params);
                break;
            case "query18":
                rawQuery = patchQuery18(rawQuery, params);
            case "query25":
                rawQuery = patchQuery25(rawQuery, params);
            default:
                // otherwise now we just return an empty map
                break;
        }
        // for now we just return the raw query, but in the future we can add some logic here to patch the query string if needed
        return new String[]{null, rawQuery};
    }

    static void fillQuery(String queryId, PreparedStatement statement, Map<String, Object> params) throws SQLException {
        switch (queryId) {
            case "query01":
                fillQuery01(statement, params);
                break;
            case "query02":
                fillQuery02(statement, params);
                break;
            case "query03":
                // no parameters
                break;
            case "query04":
                throw new IllegalArgumentException("query04 not supported");
                // break;
            case "query05":
                throw new IllegalArgumentException("query05 not supported");
                // break;
            case "query06":
                // no parameters
                break;
            case "query14":
                // no parameters
                break;
            case "query15":
                // no parameters
                break;
            case "query16":
                // no parameters
                break;
            case "query17":
                // no parameters
                break;
            default:
                // otherwise now we just return an empty map
                break;
        }
    }

    private static final String INTERVAL_PLACEHOLDER = "INTERVAL_PLACEHOLDER";
    private static String patchQuery01(String rawQuery, Map<String, Object> params) {
        Instant s = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER,
                                    (s != null ? "timestamp '" + s + "'" : "now()" ) + " - INTERVAL '1 month'");
    }

    private static String patchQuery06(String rawQuery, Map<String, Object> params) {
        Instant s = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER,
                                    (s != null ? "timestamp '" + s + "'" : "now()" ) + " - INTERVAL '10 minutes'");
    }

    private static String patchQuery14(String rawQuery, Map<String, Object> params) {
        Instant s = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER,
                                    (s != null ? "timestamp '" + s + "'" : "now()" ) + " - INTERVAL '180 days' "); 
    }

    private static String patchQuery15(String rawQuery, Map<String, Object> params) {
        Instant s = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER,
                                    (s != null ? "timestamp '" + s + "'" : "now()" ) + " - INTERVAL '180 days' "); 
    }

    private static String patchQuery16(String rawQuery, Map<String, Object> params) {
        Instant s = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER,
                                    (s != null ? "timestamp '" + s + "'" : "now()" ) + " - INTERVAL '7 days' "); 
    } 

    private static String patchQuery18(String rawQuery, Map<String, Object> params) {
        Instant s = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery
                // make sure it is in the right order. otherwise they cancel each other out
                .replaceAll(INTERVAL_PLACEHOLDER + "_INNER", 
                    (s != null ? "timestamp '" + s + "'" : "now()" ) + " - INTERVAL '24 hours' ")
                .replaceAll(INTERVAL_PLACEHOLDER,
                    (s != null ? "timestamp '" + s + "'" : "now()" ) + " - INTERVAL '7 days' ");
    } 

    private static String patchQuery25(String rawQuery, Map<String, Object> params) {
        Instant s = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery
                .replaceAll(INTERVAL_PLACEHOLDER,
                    (s != null ? "timestamp '" + s + "'" : "now()" ) + " - INTERVAL '7 days' ");
    }
 
    private static void fillQuery01(PreparedStatement statement, Map<String, Object> params) throws SQLException {
        statement.setInt(1, (Integer) params.get(RIotParameterConstants.SITE_ID));
    }

    private static void fillQuery02(PreparedStatement statement, Map<String, Object> params) throws SQLException {
        statement.setString(1, (String) params.get(RIotParameterConstants.MACHINE_ID));
    }

    private QueryFiller() {
        // prevent instantiation
    }
}
