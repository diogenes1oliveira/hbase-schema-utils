package hbase.connector.services;

import hadoop.kerberos.utils.interfaces.IOSupplier;
import hbase.connector.interfaces.HBaseConnectionProxy;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static hbase.testutils.HBaseConnectorTestUtils.startThread;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class HBaseRecreatableConnectionContextTest {
    IOSupplier<Connection> mockConnectionSupplier;
    ReadWriteLock readWriteLock;
    HBaseRecreatableConnectionContext connectionContext;
    Thread thread;

    @BeforeEach
    void setUp() {
        mockConnectionSupplier = () -> mock(Connection.class);
        readWriteLock = new ReentrantReadWriteLock();

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

        assertThat(readWriteLock.readLock().tryLock(), equalTo(true));
        assertThat(readWriteLock.writeLock().tryLock(), equalTo(false));
        readWriteLock.readLock().unlock();

        // once the context closes, I should be able to write again
        closeLatch.countDown();
        Thread.sleep(500);
        assertThat(readWriteLock.writeLock().tryLock(), equalTo(true));
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

}
