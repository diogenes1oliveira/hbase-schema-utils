package hbase.connector.interfaces;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

import static hbase.connector.utils.HBaseHelpers.isFromThisModule;

/**
 * Generic creator of HBase connections
 */
public interface HBaseConnectionFactory {
    /**
     * @param conf Hadoop-style configuration for the new connection
     * @return new connection
     * @throws IOException failed to connect
     */
    Connection create(Configuration conf) throws IOException;

    /**
     * Tests whether this factory supports the given configuration
     *
     * @param conf Hadoop-style configuration for the new connection
     * @return true if supported
     */
    boolean supports(Configuration conf);

    /**
     * Returns a priority number in case two factories support the configuration
     * <p>
     * The default implementation returns 0 for implementations from this module and 100 otherwise
     *
     * @param conf Hadoop-style configuration for the new connection
     * @return a number for the priority
     */
    default int priority(Configuration conf) {
        return isFromThisModule(this.getClass()) ? 0 : 100;
    }
}
