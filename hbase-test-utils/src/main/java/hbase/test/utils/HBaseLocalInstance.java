package hbase.test.utils;

import hbase.test.utils.interfaces.HBaseTestInstance;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

/**
 * Test instance that points to an already running local HBase instance
 */
@SuppressWarnings("java:S2139")
public class HBaseLocalInstance implements HBaseTestInstance {
    public static final String PREFIX = "test-table-";
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseLocalInstance.class);
    private static final String ZOOKEEPER_QUORUM = "localhost:2181";
    private int tableIndex = 0;
    private Properties props = null;

    /**
     * This class refers to an externally started test instance, so this method
     * just returns the ZooKeeper localhost quorum
     */
    @Override
    public void start() {
        if (props == null) {
            props = new Properties();
            props.setProperty("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        }
    }

    @Override
    public Properties properties() {
        return props;
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
        try (Connection connection = ConnectionFactory.createConnection();
             Admin admin = connection.getAdmin()) {
            TableName[] tableNames = admin.listTableNames();
            return stream(tableNames)
                    .map(TableName::getNameAsString)
                    .filter(name -> name.startsWith(PREFIX))
                    .collect(toList());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to get current temp tables", e);
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
