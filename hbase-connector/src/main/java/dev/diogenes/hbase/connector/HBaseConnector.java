package dev.diogenes.hbase.connector;

import dev.diogenes.hbase.connector.utils.ConfigEnvMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;

import static dev.diogenes.hbase.connector.utils.ConfigHelpers.*;

public class HBaseConnector {
    /**
     * Prefix to identify the config properties to be passed to HBase.
     * 
     * The default value for this prefix is in {@link #ENV_PREFIX_DEFAULT}
     */
    public static final String ENV_PREFIX_CONFIG = "hbase.connector.config.env-prefix";
    /**
     * Default value for {@link #ENV_PREFIX_CONFIG}
     */
    public static final String ENV_PREFIX_DEFAULT = "HBASE_CONF_";
    /**
     * Whether the global UGI configuration should be altered when creating a
     * connection. Defaults to {@code true}
     * 
     * For instance, when connecting to Kerberos-enabled clusters, it is necessary
     * to set the
     * {@code hadoop.security.authentication} property statically in the
     * {@link UserGroupInformation} class.
     */
    public static final String MANAGE_UGI_CONFIG = "hbase.connector.config.manage-ugi";

    private final Configuration config;
    private final boolean updateUgi;
    private volatile Connection connection;

    public HBaseConnector(Properties props) {
        this.config = toHBaseConf(props);
        this.updateUgi = isKerberized(config) && config.getBoolean(MANAGE_UGI_CONFIG, true);
    }

    public HBaseConnector(Properties props, Map<String, String> env) {
        this(mergeEnvToProps(props, env));
    }

    /**
     * Gets the current connection or creates a new one
     * 
     * If Kerberos authentication is
     *
     * @throws java.io.UncheckedIOException failure
     */
    public Connection get() {
        if (connection == null) {
            synchronized (config) {
                if (connection == null) {
                    if (updateUgi) {
                        globallyEnableKerberos();
                    }
                    try {
                        connection = ConnectionFactory.createConnection(config);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }

        return connection;
    }

    public void disconnect() {
        synchronized (config) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                connection = null;
            }
        }
    }

    private static Properties mergeEnvToProps(Properties props, Map<String, String> env) {
        String prefix = props.getProperty(ENV_PREFIX_CONFIG, ENV_PREFIX_DEFAULT);
        ConfigEnvMapper mapper = new ConfigEnvMapper(prefix);
        Properties envProps = mapper.parseEnv(env);

        return mergeProps(props, envProps);
    }
}
