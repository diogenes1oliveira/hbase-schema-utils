package hbase.connector.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Generic helper methods
 */
public final class HBaseHelpers {
    private HBaseHelpers() {
        // utility class
    }

    /**
     * Converts the properties into a HBase configuration
     *
     * @param props properties to convert
     * @return Hadoop configuration object
     */
    public static Configuration toHBaseConf(Map<String, String> props) {
        Configuration configuration = HBaseConfiguration.create();

        for (Map.Entry<String, String> entry : props.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }

        return configuration;
    }

    /**
     * Converts the properties into a HBase configuration
     *
     * @param props Java properties to convert
     * @return Hadoop configuration object
     */
    public static Configuration toHBaseConf(Properties props) {
        Map<String, String> propsMap = new HashMap<>();

        // a simple un-templated cast should work, but just in case
        for (String name : props.stringPropertyNames()) {
            String value = props.getProperty(name);
            propsMap.put(name, value);
        }

        return toHBaseConf(propsMap);
    }

    /**
     * Extracts a duration value from the Hadoop configuration
     *
     * @param conf Hadoop-style configuration for the connection
     * @param name config key to look up, the corresponding config value be an ISO-style duration
     * @return a mili-seconds value for the duration or -1 if empty
     * @throws IllegalArgumentException invalid non-empty value for the duration property
     */
    public static long getMillisDuration(Configuration conf, String name, long defaultValue) {
        String value = conf.get(name, "").trim();
        if (value.isEmpty()) {
            return defaultValue;
        } else {
            try {
                return Duration.parse(value).toMillis();
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid value for " + name, e);
            }
        }
    }

}
