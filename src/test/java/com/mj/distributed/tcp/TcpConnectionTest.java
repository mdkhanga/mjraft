package com.mj.distributed.tcp;

import com.mj.raft.testutil.Ports;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TcpConnectionTest {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
    }

    @Test
    void framesArriveInOrder() throws Exception {
        Recorder serverSide = new Recorder();
        TcpServer server = server(serverSide);
        Connection client = connect(server, new Recorder());

        for (int i = 0; i < 1000; i++) {
            assertTrue(client.send(intFrame(i)));
        }

        for (int i = 0; i < 1000; i++) {
            assertEquals(i, intOf(next(serverSide.frames)));
        }
    }

    @Test
    void largeFrameOverSocket() throws Exception {
        Recorder serverSide = new Recorder();
        TcpServer server = server(serverSide);
        Connection client = connect(server, new Recorder());
        byte[] body = new byte[1024 * 1024];
        new Random(42).nextBytes(body);
        ByteBuffer frame = ByteBuffer.allocate(4 + body.length).putInt(body.length).put(body).flip();

        client.send(frame);

        assertEquals(frame, next(serverSide.frames));
    }

    @Test
    void framesFlowInBothDirections() throws Exception {
        TcpServer server = server((c, frame) -> c.send(frame));
        Recorder clientSide = new Recorder();
        Connection client = connect(server, clientSide);

        client.send(intFrame(7));

        assertEquals(7, intOf(next(clientSide.frames)));
    }

    @Test
    void closeNotifiesBothEndsOnce() throws Exception {
        Recorder serverSide = new Recorder();
        TcpServer server = server(serverSide);
        Recorder clientSide = new Recorder();
        Connection client = connect(server, clientSide);
        Connection serverConn = next(serverSide.opened);

        client.close();
        client.close();

        assertSame(client, next(clientSide.closed));
        assertSame(serverConn, next(serverSide.closed));
        assertFalse(client.isOpen());
        assertFalse(serverConn.isOpen());
        assertFalse(client.send(intFrame(1)));
        assertNull(clientSide.closed.poll(200, TimeUnit.MILLISECONDS), "onClosed fired more than once");
    }

    @Test
    void serverCloseClosesAcceptedConnections() throws Exception {
        Recorder serverSide = new Recorder();
        TcpServer server = server(serverSide);
        Recorder first = new Recorder();
        Recorder second = new Recorder();
        connect(server, first);
        connect(server, second);
        next(serverSide.opened);
        next(serverSide.opened);

        server.close();

        next(first.closed);
        next(second.closed);
    }

    @Test
    void listenerExceptionDoesNotKillConnection() throws Exception {
        AtomicInteger seen = new AtomicInteger();
        BlockingQueue<Integer> handled = new LinkedBlockingQueue<>();
        TcpServer server = server((c, frame) -> {
            if (seen.getAndIncrement() == 0) {
                throw new RuntimeException("boom (expected by test)");
            }
            handled.add(intOf(frame));
        });
        Connection client = connect(server, new Recorder());

        client.send(intFrame(1));
        client.send(intFrame(2));

        assertEquals(2, next(handled));
        assertTrue(client.isOpen());
    }

    @Test
    void connectToClosedPortFails() throws IOException {
        int port = Ports.free(1)[0];

        assertThrows(IOException.class,
                () -> TcpClient.connect("localhost", port, CONNECT_TIMEOUT, new Recorder()));
    }

    @Test
    void bindingAPortInUseFailsImmediately() throws IOException {
        TcpServer server = server(new Recorder());

        assertThrows(IOException.class, () -> TcpServer.start("localhost", server.localPort(), new Recorder()));
    }

    private TcpServer server(ConnectionListener listener) throws IOException {
        TcpServer s = TcpServer.start("localhost", 0, listener);
        toClose.add(s);
        return s;
    }

    private Connection connect(TcpServer server, ConnectionListener listener) throws IOException {
        Connection c = TcpClient.connect("localhost", server.localPort(), CONNECT_TIMEOUT, listener);
        toClose.add(c);
        return c;
    }

    private static ByteBuffer intFrame(int value) {
        return ByteBuffer.allocate(8).putInt(4).putInt(value).flip();
    }

    private static int intOf(ByteBuffer frame) {
        return frame.getInt(4);
    }

    private static <T> T next(BlockingQueue<T> queue) throws InterruptedException {
        T item = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(item, "timed out waiting for event");
        return item;
    }

    /** Records every callback in queues so tests can wait for events without polling. */
    private static final class Recorder implements ConnectionListener {
        final BlockingQueue<Connection> opened = new LinkedBlockingQueue<>();
        final BlockingQueue<ByteBuffer> frames = new LinkedBlockingQueue<>();
        final BlockingQueue<Connection> closed = new LinkedBlockingQueue<>();

        @Override
        public void onOpened(Connection c) {
            opened.add(c);
        }

        @Override
        public void onFrame(Connection c, ByteBuffer frame) {
            frames.add(frame);
        }

        @Override
        public void onClosed(Connection c) {
            closed.add(c);
        }
    }
}
