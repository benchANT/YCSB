package site.ycsb.util;

import java.time.Instant;
import java.util.Properties;

public final class PropertyParser {

    public static Instant parseInstantProperty(Properties properties, String propertyName, Instant defaultValue) {
        String propertyValue = properties.getProperty(propertyName, defaultValue.toString());
        try {
            return Instant.parse(propertyValue);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid " + propertyName + " format: " + propertyValue, e);
        }
    }

    public static int parseIntProperty(Properties properties, String propertyName, int defaultValue) {
        String propertyValue = properties.getProperty(propertyName);
        if (propertyValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(propertyValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid int value for property '" + propertyName + "': " + propertyValue, e);
        }
    }

    // Utility method to parse a long property with a default value
    public static long parseLongProperty(Properties properties, String propertyName, long defaultValue) {
        String propertyValue = properties.getProperty(propertyName);
        if (propertyValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(propertyValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value for property '" + propertyName + "': " + propertyValue, e);
        }
    }

    private PropertyParser() {
        // Prevent instantiation
    }
}
