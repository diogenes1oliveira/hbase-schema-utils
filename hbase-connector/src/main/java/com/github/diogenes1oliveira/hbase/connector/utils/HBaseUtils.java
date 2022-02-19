package com.github.diogenes1oliveira.hbase.connector.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Generic utility aid methods
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Configuration toHBaseConf(Properties props) {
        Map<String, String> propsMap = (Map) props;
        return toHBaseConf(propsMap);
    }
}
