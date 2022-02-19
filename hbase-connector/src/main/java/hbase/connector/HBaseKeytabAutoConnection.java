package hbase.connector;

import hbase.connector.interfaces.HBaseConnectionProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tool to connect to a Kerberized HBase
 * <p>
 * This class extracts the principal and keytab as in post-2.2.0 HBase, also scheduling reconnections automatically
 */
public class HBaseKeytabAutoConnection extends HBaseConnectionProxy {
    /**
     * Kerberos principal name
     */
    public static final String CONFIG_PRINCIPAL = "hbase.client.keytab.principal";
    /**
     * Kerberos keytab file
     */
    public static final String CONFIG_KEYTAB = "hbase.client.keytab.file";
    /**
     * Automatic reconnection period
     */
    public static final String CONFIG_RECONNECTION_PERIOD = "hbase.custom.utils.reconnection.period";
    private final AtomicReference<Connection> connectionRef = new AtomicReference<>();

    public HBaseKeytabAutoConnection(Configuration conf, String principal, String keytab) {
        throw new UnsupportedOperationException();
    }

    public static boolean isKerberos(Map<String, String> connectionProps) {
        return false;
    }

    @Override
    protected Connection getConnection() {
        return connectionRef.get();
    }

}
