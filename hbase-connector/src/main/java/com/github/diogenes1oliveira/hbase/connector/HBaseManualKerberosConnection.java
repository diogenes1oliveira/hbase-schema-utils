package com.github.diogenes1oliveira.hbase.connector;

import com.github.diogenes1oliveira.hbase.connection.interfaces.HBaseConnectionProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.util.concurrent.atomic.AtomicReference;

public class HBaseManualKerberosConnection extends HBaseConnectionProxy {
    private final AtomicReference<Connection> connectionRef = new AtomicReference<>();

    public HBaseManualKerberosConnection(Configuration conf, String principal, String keytab) {

    }

    @Override
    protected Connection getConnection() {
        return connectionRef.get();
    }

}
