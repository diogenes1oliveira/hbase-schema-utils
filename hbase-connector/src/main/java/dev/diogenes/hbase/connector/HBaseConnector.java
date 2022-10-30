package dev.diogenes.hbase.connector;

import dev.diogenes.hbase.connector.utils.ConfigEnvMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;

import static dev.diogenes.hbase.connector.utils.ConfigHelpers.mergeProps;
import static dev.diogenes.hbase.connector.utils.ConfigHelpers.toHBaseConf;

public class HBaseConnector {
    public static final String ENV_PREFIXES_CONFIG = "hbase.connector.config.env-prefixes";
    public static final String ENV_PREFIXES_DEFAULT = "HBASE_";

    private final Configuration config;
    private volatile Connection connection;

    public HBaseConnector(Properties props) {
        this.config = toHBaseConf(props);
    }

    public HBaseConnector(Properties props, Map<String, String> env) {
        this(mergeEnvToProps(props, env));
    }

    /**
     * Gets the current connection or creates a new one
     *
     * @throws java.io.UncheckedIOException failure
     */
    public Connection get() {
        if (connection == null) {
            synchronized (config) {
                if (connection == null) {
                    try {
                        connection = ConnectionFactory.createConnection(config);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                return connection;
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
        String[] prefixes = props.getProperty(ENV_PREFIXES_CONFIG, ENV_PREFIXES_DEFAULT).split(",");
        ConfigEnvMapper mapper = new ConfigEnvMapper(prefixes);
        Properties envProps = mapper.parseEnv(env);

        return mergeProps(props, envProps);
    }
}
