package hbase.connector.services;

import hbase.base.interfaces.IOSupplier;
import hbase.base.exceptions.UncheckedTimeoutException;
import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.utils.TimedReadWriteLock;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static hbase.test.utils.HBaseTestHelpers.safeSleep;
import static hbase.testutils.HBaseConnectorTestUtils.startThread;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("java:S2925")
class HBaseRecreatableConnectionContextTest {
    IOSupplier<Connection> mockConnectionSupplier;
    TimedReadWriteLock readWriteLock;
    HBaseRecreatableConnectionContext connectionContext;
    Thread thread;

    @BeforeEach
    void setUp() {
        mockConnectionSupplier = () -> mock(Connection.class);
        readWriteLock = new TimedReadWriteLock(2000, 5000);

        connectionContext = new HBaseRecreatableConnectionContext(mockConnectionSupplier, readWriteLock);
        thread = null;
    }

    @AfterEach
    void cleanUp() {
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    @Test
    void enter_LocksJustForWrites() throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        thread = startThread(() -> {
            HBaseConnectionProxy proxy = connectionContext.enter();
            startLatch.countDown();
            closeLatch.await();
            proxy.close();
        });

        // within the context, I shouldn't be able to acquire the write lock
        startLatch.await();
        Thread.sleep(500);

        assertThat(readWriteLock.tryLockRead(), equalTo(true));
        assertThat(readWriteLock.tryLockWrite(), equalTo(false));
        readWriteLock.unlockRead();

        // once the context closes, I should be able to write again
        closeLatch.countDown();
        Thread.sleep(500);
        assertThat(readWriteLock.tryLockWrite(), equalTo(true));
    }

    @Test
    void enter_RecreatesConnectionJustOnce() throws IOException {
        Connection firstConnection;

        try (HBaseConnectionProxy context = connectionContext.enter()) {
            firstConnection = connectionContext.getUnproxiedConnection();
        }

        try (HBaseConnectionProxy context = connectionContext.enter()) {
            assertThat(connectionContext.getUnproxiedConnection(), sameInstance(firstConnection));
        }
    }

    @Test
    void refresh_DoesRecreateConnection() throws IOException {
        Connection firstConnection;

        try (HBaseConnectionProxy context = connectionContext.enter()) {
            firstConnection = connectionContext.getUnproxiedConnection();
        }

        connectionContext.refresh();

        try (HBaseConnectionProxy context = connectionContext.enter()) {
            assertThat(connectionContext.getUnproxiedConnection(), not(sameInstance(firstConnection)));
        }
    }

    @Test
    void disconnect_DoesCloseConnection() throws IOException {
        Connection firstConnection;

        try (HBaseConnectionProxy context = connectionContext.enter()) {
            firstConnection = connectionContext.getUnproxiedConnection();
        }

        // should keep the connection reference outside the context
        assertThat(connectionContext.getUnproxiedConnection(), sameInstance(firstConnection));

        // should close and set to null the old connection
        connectionContext.disconnect();
        connectionContext.disconnect(); // idempotency
        verify(firstConnection).close();
        assertThat(connectionContext.getUnproxiedConnection(), nullValue());
    }

    @Test
    void enter_ThrowsIfWaitedTooLong() throws IOException {
        // given a connection that takes forever to close
        Connection mockConnection = mock(Connection.class);
        doAnswer(i -> {
            safeSleep(9_000_000L);
            return null;
        }).when(mockConnection).close();

        // and a lock with low read timeout
        readWriteLock = new TimedReadWriteLock(100, 5_000L);

        // and a context
        connectionContext = new HBaseRecreatableConnectionContext(() -> mockConnection, readWriteLock);

        // create and destroy the connection, the thread hangs on the disconnect()
        startThread(() -> {
            connectionContext.refresh();
            connectionContext.disconnect();
        });
        safeSleep(500);

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertThrows(UncheckedTimeoutException.class, connectionContext::enter)
        );

    }

    @Test
    void reconnect_ThrowsIfWaitedTooLong() {
        // given a lock with low write timeout
        readWriteLock = new TimedReadWriteLock(5_000L, 100);

        // and a context
        connectionContext = new HBaseRecreatableConnectionContext(mockConnectionSupplier, readWriteLock);

        // enter the context, therefore acquiring the read lock
        startThread(() -> connectionContext.enter());
        safeSleep(500);

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertThrows(UncheckedTimeoutException.class, connectionContext::refresh)
        );

    }
}
