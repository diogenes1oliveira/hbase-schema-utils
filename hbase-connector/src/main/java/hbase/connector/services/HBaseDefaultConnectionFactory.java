package hbase.connector.services;

import hadoop.kerberos.utils.UgiGlobalContextManager;
import hadoop.kerberos.utils.interfaces.IOAuthContext;
import hbase.connector.interfaces.HBaseConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Creates standard HBase connections via {@link ConnectionFactory#createConnection(Configuration)}
 */
public class HBaseDefaultConnectionFactory implements HBaseConnectionFactory {
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
        try (IOAuthContext<UserGroupInformation> context = enterUgiContext(conf)) {
            return ConnectionFactory.createConnection(conf);
        }
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

    protected IOAuthContext<UserGroupInformation> enterUgiContext(Configuration hBaseConf) throws IOException {
        String principal = hBaseConf.getTrimmed(CONFIG_PRINCIPAL, "");
        String keytab = hBaseConf.getTrimmed(CONFIG_KEYTAB, "");

        if (principal.isEmpty() || keytab.isEmpty()) {
            return UgiGlobalContextManager.enterDefault();
        } else {
            return UgiGlobalContextManager.enterWithKeytab(principal, keytab);
        }
    }
}
