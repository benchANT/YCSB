package site.ycsb.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.benchant.realtimeiot.RIotParameterConstants;

public final class QueryFiller {
    
    private static final DateTimeFormatter dateFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneOffset.UTC);

    static String[] patchQueryString(String queryId, String rawQuery, java.util.Map<String, Object> params) {
        switch (queryId) {
            case "query01":
                rawQuery = patchQuery01(rawQuery, params);
                break;
            case "query04":
                rawQuery = patchQuery04(rawQuery, params);
                break;
            case "query06":
                rawQuery = patchQuery06(rawQuery, params);
                break;
            case "query07":
                rawQuery = patchQuery07(rawQuery, params);
                break;
            case "query08":
                rawQuery = patchQuery08(rawQuery, params);
                break;
            case "query09":
                rawQuery = patchQuery09(rawQuery, params);
                break;
            case "query10":
                rawQuery = patchQuery10(rawQuery, params);
                break;
            case "query11":
                rawQuery = patchQuery11(rawQuery, params);
                break;
            case "query12":
                rawQuery = patchQuery12(rawQuery, params);
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
            case "query17":
                rawQuery = patchQuery17(rawQuery, params);
                break;
            case "query18":
                rawQuery = patchQuery18(rawQuery, params);
                break;
            case "query21":
                rawQuery = patchQuery21(rawQuery, params);
                break;
            case "query25":
                rawQuery = patchQuery25(rawQuery, params);
                break;
            case "query28":
                rawQuery = patchQuery28(rawQuery, params);
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
            case "query02":
                fillQuery02(statement, params);
                break;
            case "query03":
                // no parameters
                break;
            case "query04":
                fillQuery04(statement, params);
                break;
            case "query05":
                throw new IllegalArgumentException("query05 not specified");
                // break;
            case "query06":
                // no parameters
                break;
            case "query07":
                // no parameters
                break;
            case "query08":
                // no parameters
                break;
            case "query09":
                // no parameters
                break;
            case "query10":
                // no parameters
                break;
            case "query11":
                // no parameters
                break;
            case "query12":
                // no parameters
                break;
            case "query13":
                throw new IllegalArgumentException("query13 not specified");
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
            case "query18":
                // no parameters
                break;
            case "query19":
                throw new IllegalArgumentException("query19 not specified");
            case "query20":
                throw new IllegalArgumentException("query20 not specified");
            case "query21":
                // no parameters
                break;
            case "query22":
                throw new IllegalArgumentException("query22 not specified");
            case "query23":
                throw new IllegalArgumentException("query23 not specified");
            case "query24":
                throw new IllegalArgumentException("query24 not specified");
            case "query25":
                // no parameters
                break;
            case "query26":
                throw new IllegalArgumentException("query26 not specified");
            case "query27":
                throw new IllegalArgumentException("query27 not specified");
            case "query28":
                // no parameters
                break;
            case "query29":
                throw new IllegalArgumentException("query29 not specified");
            case "query30":
                throw new IllegalArgumentException("query30 not specified");
            case "query31":
                throw new IllegalArgumentException("query31 not specified");
            case "query32":
                throw new IllegalArgumentException("query32 not specified");
            case "query33":
                throw new IllegalArgumentException("query33 not specified");
            default:
                // otherwise now we just return an empty map
                break;
        }
    }

    private static final String INTERVAL_PLACEHOLDER = "INTERVAL_PLACEHOLDER";
    private static String patchQuery01(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery
            .replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '1 MONTH'")
            .replaceAll("LATEST_TIMESTAMP", s); 
    }

    private static String patchQuery04(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '1 MONTH'");
    }

    private static String patchQuery06(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '10 MINUTE'");
    }

    private static String patchQuery07(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '30 DAY'");
    }

    private static String patchQuery08(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER + "_LOWER", s + " - INTERVAL '30 DAY'")
                .replaceAll(INTERVAL_PLACEHOLDER + "_UPPER", s);
    }

    private static String patchQuery09(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '30 DAY'");
    }

    private static String patchQuery10(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '30 DAY'");
    }

    private static String patchQuery11(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '30 DAY'");
    }

    private static String patchQuery12(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '30 DAY'");
    }

    private static String patchQuery14(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '180 days' "); 
    }

    private static String patchQuery15(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '180 DAY' "); 
    }

    private static String patchQuery16(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '7 DAY' "); 
    } 

    private static String patchQuery17(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '30 DAY' "); 
    } 

    private static String patchQuery18(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery
                // make sure it is in the right order. otherwise they cancel each other out
                .replaceAll(INTERVAL_PLACEHOLDER + "_INNER", s + " - INTERVAL '24 HOUR' ")
                .replaceAll(INTERVAL_PLACEHOLDER,            s + " - INTERVAL '7 DAY' ");
    } 

    private static String patchQuery21(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery
                // make sure it is in the right order. otherwise they cancel each other out
                .replaceAll(INTERVAL_PLACEHOLDER + "_1", s + " - INTERVAL '14 DAY' ")
                .replaceAll(INTERVAL_PLACEHOLDER + "_2", s + " - INTERVAL '28 DAY' ");
    } 

    private static String patchQuery25(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '7 DAY'");
    }

    private static String patchQuery28(String rawQuery, Map<String, Object> params) {
        Instant t = (Instant) params.get(RIotParameterConstants.TIMESTAMP_UPPER);
        String s = t == null ? "now()" : "toDateTime64('" + dateFormatter.format(t) + "', 3, 'UTC')";
        params.remove(RIotParameterConstants.TIMESTAMP_UPPER);
        return rawQuery.replaceAll(INTERVAL_PLACEHOLDER, s + " - INTERVAL '30 DAY'");
    }
 
    private static void fillQuery01(PreparedStatement statement, Map<String, Object> params) throws SQLException {
        statement.setInt(1, (Integer) params.get(RIotParameterConstants.SITE_ID));
    }

    private static void fillQuery02(PreparedStatement statement, Map<String, Object> params) throws SQLException {
        statement.setString(1, (String) params.get(RIotParameterConstants.MACHINE_ID));
    }

    private static void fillQuery04(PreparedStatement statement, Map<String, Object> params) throws SQLException {
        statement.setString(1, (String) params.get(RIotParameterConstants.MACHINE_ID));
    }

    private QueryFiller() {
        // prevent instantiation
    }
}
