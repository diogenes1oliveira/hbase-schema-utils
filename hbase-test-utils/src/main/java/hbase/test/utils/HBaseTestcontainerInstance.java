package hbase.test.utils;

import hbase.test.utils.interfaces.HBaseTestInstance;
import io.github.diogenes1oliveira.hbase2.HBaseContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.Properties;

/**
 * Test instance that spins up a HBase Testcontainer
 */
public class HBaseTestcontainerInstance implements HBaseTestInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTestcontainerInstance.class);

    public static final String IMAGE = "diogenes1oliveira/hbase2-docker:1.0.0-hbase2.0.2";
    public static final String PREFIX = "test-table-";

    private HBaseContainer container;
    private int tableIndex = 0;

    /**
     * Spins up a Testcontainer for HBase and returns the connection properties
     */
    @Override
    public Properties start() {
        container = new HBaseContainer(IMAGE);
        container.start();
        container.followOutput(new Slf4jLogConsumer(LOGGER));

        return container.getProperties();
    }

    /**
     * Stops the Testcontainers
     */
    @Override
    public void close() {
        container.stop();
    }

    /**
     * Returns a new unique table name
     */
    @Override
    public String tempTableName() {
        return PREFIX + tableIndex++;
    }

    /**
     * Gets a unique name for this test instance, so it can be selected dynamically
     * <p>
     * Returns the string "docker"
     */
    @Override
    public String name() {
        return "docker";
    }
}
