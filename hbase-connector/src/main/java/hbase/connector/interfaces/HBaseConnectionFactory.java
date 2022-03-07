package hbase.connector.interfaces;

import hbase.base.interfaces.Service;
import hbase.base.services.ServiceRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * Generic creator of HBase connections
 */
public interface HBaseConnectionFactory extends Service {
    /**
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
     * Instantiates a factory that can handle the given config using the global {@link ServiceRegistry}
     *
     * @param conf Hadoop-style configuration for the new connection
     * @return factory instance
     * @throws IllegalArgumentException no factory available
     */
    static HBaseConnectionFactory get(Configuration conf) {
        return ServiceRegistry.findService(
                HBaseConnectionFactory.class,
                f -> f.supports(conf)
        );
    }
}
