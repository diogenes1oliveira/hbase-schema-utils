package hbase.connector.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Generic helper methods
 */
public final class HBaseUtils {
    private HBaseUtils() {
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

}
