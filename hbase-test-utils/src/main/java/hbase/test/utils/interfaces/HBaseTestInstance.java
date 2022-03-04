package hbase.test.utils.interfaces;

import java.io.IOException;
import java.util.Properties;

/**
 * Generic interface for a provider of HBase test instances
 */
public interface HBaseTestInstance extends AutoCloseable {
    /**
     * Starts this test instance
     *
     * @return properties to connect to the test instance
     */
    Properties start() throws IOException;

    /**
     * Closes this test instance
     * <p>
     * The default implementation calls {@link #cleanUp()}, so be sure to
     * call it yourself if you customize this method
     */
    @Override
    default void close() throws IOException {
        cleanUp();
    }

    /**
     * Yields a new test table temp name
     */
    String tempTableName();

    /**
     * Executes a clean-up of the created test tables
     */
    default void cleanUp() throws IOException {
        // nothing to do by default
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
