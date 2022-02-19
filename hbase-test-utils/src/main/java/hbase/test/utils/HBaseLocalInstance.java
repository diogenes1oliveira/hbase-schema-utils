package hbase.test.utils;

import hbase.connector.HBaseConnector;
import hbase.test.utils.interfaces.HBaseTestInstance;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

import static hbase.test.utils.HBaseTestHelpers.safeDropTables;

/**
 * Test instance that points to an already running local HBase instance
 */
public class HBaseLocalInstance implements HBaseTestInstance {

    private static final String ZOOKEEPER_QUORUM = "localhost:2181";
    public static final String PREFIX = "test-table-";
    private int tableIndex = 0;
    private Properties props;
    private HBaseConnector connector;

    /**
     * This class refers to an externally started test instance, so this method
     * just returns the ZooKeeper localhost quorum
     */
    @Override
    public Properties start() {
        props = new Properties();
        props.setProperty("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);

        return props;
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

        try (HBaseConnector connector = new HBaseConnector(props);
             Admin admin = connector.connect().getAdmin()) {
            safeDropTables(admin, tempNames);
        }
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
