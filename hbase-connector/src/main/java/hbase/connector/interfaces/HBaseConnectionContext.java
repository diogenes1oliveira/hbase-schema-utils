package hbase.connector.interfaces;

import org.apache.hadoop.hbase.client.Connection;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

/**
 * A context for HBase connections compatible with the try-with-resources syntax
 */
public interface HBaseConnectionContext {
    /**
     * Initializes and enters the connection context
     *
     * @return connection proxy
     * @throws IOException failed to enter the connection context
     */
    HBaseConnectionProxy enter() throws IOException;

    /**
     * Gets the original HBase connection object, might be null if not initialized yet
     */
    @Nullable
    Connection getUnproxiedConnection();

    /**
     * Refreshes the underlying HBase connection
     */
    void refresh() throws IOException;

    /**
     * Closes the underlying HBase connection
     */
    void disconnect() throws IOException;
}
