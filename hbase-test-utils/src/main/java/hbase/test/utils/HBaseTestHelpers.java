package hbase.test.utils;

import hbase.connector.HBaseConnector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

/**
 * Miscellaneous utilities to help with tests
 */
public final class HBaseTestHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTestHelpers.class);

    private static final int RETRY_MILLIS = 100;
    private static final int DEFAULT_RETRY_COUNT = 3;

    private HBaseTestHelpers() {
        // utility class
    }

    /**
     * Creates a new table descriptor from string values
     *
     * @param name           name of the new table
     * @param columnFamilies UTF-8 encoded names of the column families
     * @return table descriptor
     */
    public static TableDescriptor newTableDescriptor(String name, String... columnFamilies) {
        byte[][] familyBytes = new byte[columnFamilies.length][];
        for (int i = 0; i < columnFamilies.length; ++i) {
            familyBytes[i] = columnFamilies[i].getBytes(StandardCharsets.UTF_8);
        }

        return newTableDescriptor(TableName.valueOf(name), familyBytes);
    }

    public static TableDescriptor newTableDescriptor(TableName tableName, byte[]... columnFamilies) {
        List<ColumnFamilyDescriptor> familyDescriptors = stream(columnFamilies)
                .map(HBaseTestHelpers::newColumnFamilyDescriptor)
                .collect(toList());
        return TableDescriptorBuilder
                .newBuilder(tableName)
                .setColumnFamilies(familyDescriptors)
                .build();
    }

    public static TableDescriptor newTableDescriptor(TableName tableName, String... columnFamilies) {
        List<ColumnFamilyDescriptor> familyDescriptors = stream(columnFamilies)
                .map(HBaseTestHelpers::newColumnFamilyDescriptor)
                .collect(toList());
        return TableDescriptorBuilder
                .newBuilder(tableName)
                .setColumnFamilies(familyDescriptors)
                .build();
    }

    public static void createTable(Admin admin, TableDescriptor descriptor) {
        try {
            long t0 = System.nanoTime();
            LOGGER.info("Creating table {}", descriptor.getTableName());
            admin.createTable(descriptor);
            LOGGER.info("Table {} created in {}ms", descriptor.getTableName(), millisSince(t0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void createTable(Connection connection, TableDescriptor descriptor) {
        try (Admin admin = connection.getAdmin()) {
            createTable(admin, descriptor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void createTable(HBaseConnector connector, TableDescriptor descriptor) {
        try (Connection connection = connector.connect();
             Admin admin = connection.getAdmin()) {
            createTable(admin, descriptor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a new standard UTF-8 encoded column family descriptor
     */
    public static ColumnFamilyDescriptor newColumnFamilyDescriptor(String familyName) {
        return newColumnFamilyDescriptor(familyName.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates a new standard UTF-8 encoded column family descriptor
     */
    public static ColumnFamilyDescriptor newColumnFamilyDescriptor(byte[] family) {
        return ColumnFamilyDescriptorBuilder.newBuilder(family).build();
    }

    /**
     * Drops all given tables in parallel
     *
     * @param admin admin instance
     * @param names names of the tables to drop
     */
    public static void safeDropTables(Admin admin, String... names) {
        long t0 = System.nanoTime();
        LOGGER.warn("Dropping all temp tables");
        asList(names).parallelStream().forEach(name -> {
            LOGGER.warn("Dropping table {}", name);
            TableName tableName = TableName.valueOf(name);
            try {
                safeDisableTable(admin, tableName, DEFAULT_RETRY_COUNT);
                safeDeleteTable(admin, tableName, DEFAULT_RETRY_COUNT);
            } catch (RuntimeException e) {
                LOGGER.error("Failed to drop table", e);
            }
            LOGGER.warn("Table dropped: {}", name);
        });
        LOGGER.warn("All temp tables were dropped within {}ms", millisSince(t0));
    }

    /**
     * Retriably disabling a table in HBase
     *
     * @param admin     admin instance
     * @param tableName name of the table to drop
     * @param retries   number of times to retry the operation before giving up
     * @throws IllegalStateException when retries < 0
     */
    public static void safeDisableTable(Admin admin, TableName tableName, int retries) {
        if (retries < 0) {
            throw new IllegalStateException("Failed too many times");
        }
        try {
            if (admin.isTableDisabled(tableName)) {
                safeSleep(200);
            }
            admin.disableTable(tableName);
        } catch (TableNotEnabledException e) {
            // done
        } catch (IOException e) {
            safeDisableTable(admin, tableName, retries - 1);
        }
    }

    /**
     * Retriable deletion of a table in HBase
     *
     * @param admin     admin instance
     * @param tableName name of the table to delete
     * @param retries   number of times to retry the operation before giving up
     * @throws IllegalStateException when retries < 0
     */
    public static void safeDeleteTable(Admin admin, TableName tableName, int retries) {
        if (retries < 0) {
            throw new IllegalStateException("Failed too many times");
        }
        try {
            if (!admin.tableExists(tableName)) {
                safeSleep(RETRY_MILLIS);
            }
            admin.deleteTable(tableName);
        } catch (TableNotDisabledException e) {
            throw new IllegalStateException("Table should be disabled beforehand", e);
        } catch (TableNotFoundException e) {
            // done
        } catch (IOException e) {
            safeDisableTable(admin, tableName, retries - 1);
        }
    }

    /**
     * Wraps over {@link Thread#sleep(long)} and handles its checked exception
     *
     * @param millis time in milliseconds to sleep
     * @throws IllegalStateException interruption request while sleeping. The current thread is interrupted
     *                               before throwing this exception
     */
    public static void safeSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOGGER.warn("Thread interrupted while sleeping", e);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted while sleeping", e);
        }
    }

    /**
     * Loads the .properties object from the classpath
     *
     * @param name name of the resource to fetch
     * @return parsed Properties object
     */
    public static Properties loadPropsFromResource(String name) throws IOException {
        Properties props = new Properties();

        try (InputStream stream = HBaseTestHelpers.class.getClassLoader().getResourceAsStream(name)) {
            if (stream == null) {
                throw new IOException("No such resource: " + name);
            }
            props.load(stream);
        }

        return props;
    }

    private static double millisSince(long t0Nanos) {
        long delta = System.nanoTime() - t0Nanos;
        return delta / 1.0e6;
    }
}
