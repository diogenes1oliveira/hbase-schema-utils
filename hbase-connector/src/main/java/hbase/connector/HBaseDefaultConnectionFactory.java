package hbase.connector;

import hbase.connector.interfaces.HBaseConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Creates standard HBase connections via {@link ConnectionFactory#createConnection(Configuration)}
 */
public class HBaseDefaultConnectionFactory implements HBaseConnectionFactory {
    /**
     * @param conf Hadoop-style configuration for the new connection
     * @return the new connection
     * @throws IOException failed to connect
     */
    @Override
    public Connection create(Configuration conf) throws IOException {
        return ConnectionFactory.createConnection(conf);
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
