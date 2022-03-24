package hbase.test.utils.interfaces;

import hbase.test.utils.HBaseTestHelpers;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static hbase.test.utils.HBaseTestHelpers.safeDropTables;

/**
 * Generic interface for a provider of HBase test instances
 */
public interface HBaseTestInstance extends AutoCloseable {
    /**
     * Starts this test instance in an idempotent fashion
     */
    void start() throws IOException;

    /**
     * Properties to connect to the test instance
     */
    Properties properties();

    /**
     * Closes this test instance
     * <p>
     * The default implementation does nothing
     */
    @Override
    default void close() throws IOException {
        // nothing to do
    }

    /**
     * Yields a new test table temp name
     */
    String tempTableName();

    /**
     * List of temporary tables in the test instance
     */
    List<String> tempTableNames();

    /**
     * Executes a clean-up of the created test tables
     */
    default void cleanUp() {
        List<String> tempNames = this.tempTableNames();
        Properties props = this.properties();

        try (Connection connection = HBaseTestHelpers.newConnection(props);
             Admin admin = connection.getAdmin()) {
            safeDropTables(admin, tempNames.toArray(new String[0]));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to drop current temp tables", e);
        }
    }

    /**
     * Gets a unique name for this test instance, so it can be selected dynamically
     * <p>
     * The default implementation returns the simple class name
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
