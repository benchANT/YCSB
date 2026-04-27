package site.ycsb.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

import com.benchant.realtimeiot.RIotParameterConstants;

public final class QueryFiller {
    
    static String[] patchQueryString(String queryId, String rawQuery, java.util.Map<String, Object> params) {
        switch (queryId) {
            case "query01":
                rawQuery = patchQuery01(rawQuery, params);
                break;
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

    private static void fillQuery01(PreparedStatement statement, Map<String, Object> params) throws SQLException {
        statement.setInt(1, (Integer) params.get(RIotParameterConstants.SITE_ID));
    }




    private QueryFiller() {
        // prevent instantiation
    }
}
