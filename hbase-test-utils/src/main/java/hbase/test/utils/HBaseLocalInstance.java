package hbase.test.utils;

import hbase.connector.HBaseConnector;
import hbase.test.utils.interfaces.HBaseTestInstance;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

import static hbase.test.utils.HBaseTestHelpers.safeDropTables;
import static java.util.Arrays.stream;

/**
 * Test instance that points to an already running local HBase instance
 */
public class HBaseLocalInstance implements HBaseTestInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseLocalInstance.class);

    private static final String ZOOKEEPER_QUORUM = "localhost:2181";
    public static final String PREFIX = "test-table-";
    private int tableIndex = 0;
    private HBaseConnector connector;

    /**
     * This class refers to an externally started test instance, so this method
     * just returns the ZooKeeper localhost quorum
     */
    @Override
    public Properties start() {
        Properties props = new Properties();
        props.setProperty("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);

        connector = new HBaseConnector(props);
        cleanUpCurrent();
        return props;
    }

    private void cleanUpCurrent() {
        try (Connection connection = connector.connect();
             Admin admin = connection.getAdmin()) {
            TableName[] tableNames = admin.listTableNames();
            String[] tempNames = stream(tableNames)
                    .map(TableName::getNameAsString)
                    .filter(name -> name.startsWith(PREFIX))
                    .toArray(String[]::new);
            safeDropTables(admin, tempNames);
        } catch (Exception e) {
            LOGGER.error("Failed to drop current temp tables", e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Gets the connector set up to talk to a local HBase instance
     */
    public HBaseConnector connector() {
        return connector;
    }

    /**
     * Returns a new unique table name
     */
    @Override
    public String tempTableName() {
        return PREFIX + tableIndex++;
    }

    /**
     * Drops all tables ever returned by {@link #tempTableName()}
     *
     * @throws IOException           failed to get the Admin instance for the connection
     * @throws IllegalStateException failed to drop the temporary tables
     */
    @Override
    public void cleanUp() throws IOException {
        String[] tempNames = IntStream.range(0, tableIndex)
                .mapToObj(i -> PREFIX + i)
                .toArray(String[]::new);

        try (Connection connection = connector.connect();
             Admin admin = connection.getAdmin()) {
            safeDropTables(admin, tempNames);
        }
    }

    @Override
    public void close() throws IOException {
        cleanUp();
        connector.close();
    }

    /**
     * Gets a unique name for this test instance, so it can be selected dynamically
     * <p>
     * Returns the string "docker"
     */
    @Override
    public String name() {
        return "local";
    }
}
