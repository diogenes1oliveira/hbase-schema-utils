package com.github.diogenes1oliveira.hbase.schema.utils;

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
     * Converts the binary value array into a Long instance
     *
     * @param value binary value
     * @return converted long value
     * @throws IllegalArgumentException value null or not with 8 bytes
     */
    public static long bytesToLong(byte[] value) {
        if (value == null || value.length != 8) {
            throw new IllegalArgumentException("Invalid Long value");
        }
        return Bytes.toLong(value);
    }

    /**
     * Builds a new map keyed by binary bytes
     *
     * @param <T> value type
     * @return map sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static <T> SortedMap<byte[], T> sortedByteMap() {
        return new TreeMap<>(Bytes.BYTES_COMPARATOR);
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
