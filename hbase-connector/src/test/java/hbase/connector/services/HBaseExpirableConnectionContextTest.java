package hbase.connector.services;

import hadoop.kerberos.utils.interfaces.IOSupplier;
import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.utils.TimedReadWriteLock;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class HBaseExpirableConnectionContextTest {
    IOSupplier<Connection> mockConnectionSupplier;
    TimedReadWriteLock readWriteLock;

    @BeforeEach
    void setUp() {
        mockConnectionSupplier = () -> mock(Connection.class);
        readWriteLock = new TimedReadWriteLock(5_000L, 10_000L);

    }

    @Test
    void enter_ReconnectsIfExpired() throws InterruptedException, IOException {
        HBaseRecreatableConnectionContext connectionContext = new HBaseExpirableConnectionContext(
                400,
                mockConnectionSupplier,
                readWriteLock
        );
        Connection firstConnection;

        try (HBaseConnectionProxy context = connectionContext.enter()) {
            firstConnection = connectionContext.getUnproxiedConnection();
        }

        // shouldn't recreate, not expired yet
        try (HBaseConnectionProxy context = connectionContext.enter()) {
            Connection secondConnection = connectionContext.getUnproxiedConnection();

            assertThat(secondConnection, sameInstance(firstConnection));
            verify(firstConnection, never()).close();
        }

        Thread.sleep(500);

        // should recreate, already expired
        try (HBaseConnectionProxy context = connectionContext.enter()) {
            Connection afterSleepConnection = connectionContext.getUnproxiedConnection();

            assertThat(afterSleepConnection, not(sameInstance(firstConnection)));
            verify(firstConnection).close();
        }
    }
}
