package com.github.diogenes1oliveira.hbase.schema.impl;

import com.github.diogenes1oliveira.hbase.connection.interfaces.HBaseConnectionProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.VersionInfo;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HBaseConnection extends HBaseConnectionProxy {
    public static final String CONFIG_PRINCIPAL = "hbase.client.keytab.principal";
    public static final String CONFIG_KEYTAB = "hbase.client.keytab.file";
    public static final String CONFIG_RECONNECTION_PERIOD = "";
    public HBaseConnection(Properties properties) {
        VersionInfo.getBuildVersion()
    }

    public static Connection createConnection(Map<String, String> props) {

    }
    public static Connection createBasicConnection(Configuration conf) throws IOException {
        return ConnectionFactory.createConnection(conf);
    }
    public static Connection createKeyTabConnection(Configuration conf, String user, String keyTab) throws IOException {
        return ConnectionFactory.createConnection(conf);
    }
}
