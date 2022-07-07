package hbase.connector.services;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import hbase.connector.interfaces.HBaseConnectionProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Object to manage a pool of user-scoped HBase connections
 */
public class HBaseConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnector.class);

    public static final String CONFIG_HBASE_AUTH = "hbase.security.authentication";
    public static final String CONFIG_HADOOP_AUTH = "hadoop.security.authentication";
    /**
     * Duration in milliseconds for the timeout of an expiration
     */
    public static final String CONFIG_HBASE_EXPIRATION = "hbase.custom.connection.expire";
    /**
     * Default value for {@code hbase.custom.connection.max}
     */
    public static final String CONFIG_HBASE_EXPIRATION_DEFAULT = "86400000"; // 24 hours
    /**
     * Maximum number of connections to keep in cache
     */
    public static final String CONFIG_HBASE_MAX_COUNT = "hbase.custom.connection.max.count";
    /**
     * Default value for {@code hbase.custom.connection.max.count}
     */
    public static final String CONFIG_HBASE_MAX_COUNT_DEFAULT = "20";
    /**
     * Path to the directory holding the keytab files
     */
    public static final String CONFIG_KEYTABS_PATH = "hbase.custom.keytabs.path";
    /**
     * Default value for {@code hbase.custom.keytabs.path}
     */
    public static final String CONFIG_KEYTABS_PATH_DEFAULT = "/etc/security/keytabs";
    /**
     * Regular expression mapping principal to a keytab name
     */
    public static final String CONFIG_KEYTABS_MAPPING = "hbase.custom.keytabs.mapping";
    /**
     * Default value for {@code hbase.custom.keytabs.mapping}
     */
    public static final String CONFIG_KEYTABS_MAPPING_DEFAULT = "(<?name>[a-z0-9._-]+)@[A-Z0-9.-]+";
    private static final String KERBEROS_AUTH = "kerberos";

    private final Configuration hBaseConf;
    private final Pattern keytabsMapping;
    private final Path keytabsPath;
    private final boolean kerberosEnabled;

    private final LoadingCache<String, Connection> connectionsCache;
    private final LoadingCache<String, UserGroupInformation> usersCache;

    /**
     * @param expireMs       duration in milliseconds for the timeout of an expiration
     * @param maxCount       maximum number of connections to keep in cache
     * @param keytabsMapping regular expression mapping principal to a keytab name
     * @param keytabsPath    path to the directory holding the keytab files
     * @param hBaseConf      Hadoop configuration object for the new connections
     */
    public HBaseConnector(long expireMs, int maxCount, Pattern keytabsMapping, Path keytabsPath, Configuration hBaseConf) {
        if (KERBEROS_AUTH.equals(hBaseConf.get(CONFIG_HADOOP_AUTH))) {
            UserGroupInformation.setConfiguration(hBaseConf);
            this.kerberosEnabled = true;
        } else {
            this.kerberosEnabled = false;
        }

        this.keytabsMapping = keytabsMapping;
        this.keytabsPath = keytabsPath;
        this.hBaseConf = hBaseConf;

        this.connectionsCache = Caffeine.newBuilder()
                                        .maximumSize(maxCount)
                                        .expireAfterWrite(expireMs, TimeUnit.MILLISECONDS)
                                        .removalListener(HBaseConnector::onRemoval)
                                        .build(this::connect);
        this.usersCache = Caffeine.newBuilder()
                                  .expireAfterWrite(expireMs, TimeUnit.MILLISECONDS)
                                  .build(this::loginFromKeyTab);
    }

    /**
     * @param props Java properties for the new connections
     */
    public HBaseConnector(Properties props) {
        this(
                Long.parseUnsignedLong(props.getProperty(CONFIG_HBASE_EXPIRATION, CONFIG_HBASE_EXPIRATION_DEFAULT)),
                Integer.parseUnsignedInt(props.getProperty(CONFIG_HBASE_MAX_COUNT, CONFIG_HBASE_MAX_COUNT_DEFAULT)),
                Pattern.compile(props.getProperty(CONFIG_KEYTABS_MAPPING, CONFIG_KEYTABS_MAPPING_DEFAULT)),
                Paths.get(props.getProperty(CONFIG_KEYTABS_PATH, CONFIG_KEYTABS_PATH_DEFAULT)),
                toConfiguration(props)
        );
    }

    private static void onRemoval(String userName, Connection connection, RemovalCause cause) {
        LOGGER.info("Cleaning up connection for user {} due to {}", userName, cause);

        if (connection == null || connection.isClosed()) {
            LOGGER.info("Null or already closed connection for user {}, nothing to do", userName);
            return;
        }
        try {
            LOGGER.info("Closing connection for user {}", userName);
            connection.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Finds a matching keytab for the given username and logs it in
     *
     * @param principal name of the principal to be logged in
     * @return Hadoop UGI object
     * @throws IllegalArgumentException principal couldn't be mapped to a keytab name
     * @throws NoSuchElementException   corresponding keytab file doesn't exist
     * @throws UncheckedIOException     failed to login with the principal and keytab
     */
    public UserGroupInformation loginFromKeyTab(String principal) {
        Matcher m = keytabsMapping.matcher(principal);
        if (!m.matches()) {
            throw new IllegalArgumentException("Principal couldn't be mapped to a keytab");
        }
        String name = m.group("name");
        Path keytabPath = keytabsPath.resolve(name + ".keytab");

        if (keytabPath.toFile().isFile()) {
            throw new NoSuchElementException("Keytab not found for principal");
        }

        try {
            LOGGER.info("Logging principal {} using keytab {}", principal, keytabPath);
            return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath.toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Gets the currently cached connection or refreshes it if expired or non-existing
     *
     * @param userName username, must be a principal if Kerberos is enabled
     * @return cached or (re)created connection
     * @throws UncheckedIOException failed to open or get a HBase connection for the user
     */
    public Connection get(String userName) {
        LOGGER.debug("Looking up connection for user {}", userName);
        Connection connection = connectionsCache.get(userName);

        return new HBaseConnectionProxy(connection) {
            @Override
            public void close() throws IOException {
                LOGGER.debug("Removing connection for user {}", userName);
                usersCache.invalidate(userName);
                super.close();
            }
        };
    }

    /**
     * Creates a new connection for the named user
     *
     * @param userName username, must be a principal if Kerberos is enabled
     * @return created connection
     * @throws UncheckedIOException     failed to open a HBase connection for the user
     * @throws IllegalArgumentException no credentials for the given principal
     */
    public Connection connect(String userName) {
        if (!kerberosEnabled) {
            LOGGER.info("Creating default connection for user {}", userName);
            return connectDefault();
        }
        LOGGER.info("Looking up credentials for the user {}", userName);
        UserGroupInformation ugi = usersCache.get(userName);
        if (ugi == null) {
            throw new IllegalArgumentException("No credentials for username");
        }
        return connect(ugi);
    }

    /**
     * Creates a new connection for the Hadoop user
     *
     * @param ugi Hadoop UGI object
     * @return created connection
     * @throws UncheckedIOException failed to open a HBase connection for the user
     */
    public Connection connect(UserGroupInformation ugi) {
        LOGGER.info("Creating HBase connection for Hadoop user {}", ugi);
        try {
            return ConnectionFactory.createConnection(hBaseConf, User.create(ugi));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a new connection using the default configuration
     *
     * @return created connection
     * @throws UncheckedIOException failed to open a HBase connection
     */
    public Connection connectDefault() {
        LOGGER.info("Creating default HBase connection");
        try {
            return ConnectionFactory.createConnection(hBaseConf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Closes any cached connections for the given user
     */
    public void close(String userName) {
        LOGGER.debug("Invalidating connection for user {}", userName);
        connectionsCache.invalidate(userName);
        usersCache.invalidate(userName);
    }

    /**
     * Closes all cached connections
     */
    public void closeAll() {
        connectionsCache.invalidateAll();
        usersCache.invalidateAll();
    }

    /**
     * Converts the Java properties into a Hadoop configuration object
     */
    public static Configuration toConfiguration(Properties props) {
        Configuration conf = HBaseConfiguration.create();

        for (String propName : props.stringPropertyNames()) {
            String propValue = props.getProperty(propName);
            conf.set(propName, propValue);
        }

        if (KERBEROS_AUTH.equals(props.getProperty(CONFIG_HBASE_AUTH))) {
            conf.set(CONFIG_HADOOP_AUTH, KERBEROS_AUTH);
        }

        return conf;
    }
}
