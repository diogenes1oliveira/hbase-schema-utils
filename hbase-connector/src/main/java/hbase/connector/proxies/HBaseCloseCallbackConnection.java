package hbase.connector.proxies;

import hbase.connector.interfaces.HBaseConnectionSafeProxy;
import hbase.connector.interfaces.IOConsumer;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * Wraps over a Connection to run some code after closing
 */
public class HBaseCloseCallbackConnection extends HBaseConnectionSafeProxy {
    private final Connection connection;
    private final IOConsumer<Connection> onClose;

    /**
     * @param connection wrapped connection object
     * @param onClose    code to execute after closing
     */
    public HBaseCloseCallbackConnection(Connection connection, IOConsumer<Connection> onClose) {
        this.onClose = onClose;
        this.connection = connection;
    }

    /**
     * Just calls the {@link #onClose} callback method
     */
    @Override
    public void close() throws IOException {
        onClose.accept(connection);
    }

    @Override
    protected Connection getWrappedConnection() {
        return connection;
    }

}
