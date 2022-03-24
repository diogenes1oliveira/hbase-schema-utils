package hbase.connector.services;

import hadoop.kerberos.utils.UgiContextManager;
import hbase.connector.interfaces.HBaseConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Creates standard HBase connections via {@link ConnectionFactory#createConnection(Configuration)}
 */
public class HBaseDefaultConnectionFactory implements HBaseConnectionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDefaultConnectionFactory.class);

    /**
     * Kerberos principal name
     */
    public static final String CONFIG_PRINCIPAL = "hbase.client.keytab.principal";
    /**
     * Kerberos keytab file
     */
    public static final String CONFIG_KEYTAB = "hbase.client.keytab.file";

    /**
     * @return the new connection
     * @throws IOException failed to connect
     */
    @Override
    public Connection create(Configuration conf) throws IOException {
        String principal = conf.getTrimmed(CONFIG_PRINCIPAL, "");
        String keyTab = conf.getTrimmed(CONFIG_KEYTAB, "");

        Connection connection;

        if (principal.isEmpty() || keyTab.isEmpty()) {
            LOGGER.info("Creating default connection");
            connection = UgiContextManager.enterDefault(user -> ConnectionFactory.createConnection(conf));
        } else {
            LOGGER.info("Creating Kerberos connection from keytab");
            UgiContextManager.enableKerberos();
            connection = UgiContextManager.enterWithKeytab(principal, keyTab, user -> ConnectionFactory.createConnection(conf));
        }

        LOGGER.info("Connection created successfully");
        return connection;
    }

    /**
     * Tests whether this factory supports the given configuration
     *
     * @param conf Hadoop-style configuration for the new connection
     * @return always {@code true}
     */
    @Override
    public boolean supports(Configuration conf) {
        return true;
    }

}
