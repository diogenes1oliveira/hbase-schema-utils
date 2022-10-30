package dev.diogenes.hbase.connector.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Generic helper methods
 */
public final class ConfigHelpers {
    private ConfigHelpers() {
        // utility class
    }

    /**
     * Converts the properties into a HBase configuration
     *
     * @param props Java properties to convert
     * @return Hadoop's configuration object
     */
    public static Configuration toHBaseConf(Properties props) {
        Configuration config = HBaseConfiguration.create();

        for (String name : props.stringPropertyNames()) {
            String value = props.getProperty(name);
            config.set(name, value);
        }

        return config;
    }

    public static Properties mergeProps(Properties... props) {
        Properties result = new Properties();

        for (Properties p : props) {
            result.putAll(p);
        }

        return result;
    }

    public static boolean isKerberized(Configuration hBaseConf) {
        String value = hBaseConf.get("hbase.security.authentication", "");
        return "kerberos".equalsIgnoreCase(value);
    }

    public static void globallyEnableKerberos() {
        Configuration newConfig = HBaseConfiguration.create();
        newConfig.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(newConfig);
    }
}
