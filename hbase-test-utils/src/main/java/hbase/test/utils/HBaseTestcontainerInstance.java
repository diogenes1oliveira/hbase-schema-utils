package hbase.test.utils;

import hbase.test.utils.interfaces.HBaseTestInstance;
import io.github.diogenes1oliveira.hbase2.HBaseContainer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.List;
import java.util.Properties;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

/**
 * Test instance that spins up a HBase Testcontainer
 */
public class HBaseTestcontainerInstance implements HBaseTestInstance {
    public static final String IMAGE = "diogenes1oliveira/hbase2-docker:1.0.0-hbase2.0.2";
    public static final String PREFIX = "test-table-";
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTestcontainerInstance.class);
    private HBaseContainer container = null;
    private Properties props = null;
    private int tableIndex = 0;

    /**
     * Spins up a Testcontainer for HBase and returns the connection properties
     */
    @Override
    public void start() {
        if (container == null) {
            container = new HBaseContainer(IMAGE);
            container.start();
            container.followOutput(new Slf4jLogConsumer(LOGGER));
        }
        props = container.getProperties();
    }

    @Override
    public Properties properties() {
        return props;
    }

    /**
     * Stops the Testcontainers
     */
    @Override
    public void close() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }

    /**
     * Returns a new unique table name
     */
    @Override
    public String tempTableName() {
        return PREFIX + tableIndex++;
    }

    @Override
    public List<String> tempTableNames() {
        try (Connection connection = HBaseTestHelpers.newConnection(props);
             Admin admin = connection.getAdmin()) {
            TableName[] tableNames = admin.listTableNames();
            return stream(tableNames)
                    .map(TableName::getNameAsString)
                    .filter(name -> name.startsWith(PREFIX))
                    .collect(toList());
        } catch (Exception e) {
            LOGGER.error("Failed to drop current temp tables", e);
            throw new IllegalStateException("Failed to drop current temp tables", e);
        }
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
