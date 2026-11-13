package com.mj.raft.client;

import com.mj.distributed.message.Response;
import com.mj.distributed.model.Redirect;
import com.mj.distributed.tcp.FrameCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * RaftClient against fake servers that misbehave. No real PeerServer, so these are fast unit tests.
 */
class RaftClientTimeoutTest {

    private static final Duration RESPONSE_TIMEOUT = Duration.ofMillis(500);
    private static final Duration TEST_LIMIT = Duration.ofSeconds(5);

    private final List<AutoCloseable> toClose = new CopyOnWriteArrayList<>(); // fake servers add accepted sockets

    @AfterEach
    void closeAll() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
    }

    @Test
    void connectTimesOutWhenServerNeverReplies() throws Exception {
        int port = fakeServer(0);
        RaftClient rc = client(port);

        assertTimeoutPreemptively(TEST_LIMIT, () -> {
            assertThrows(SocketTimeoutException.class, rc::connect);
        });
    }

    @Test
    void getTimesOutWhenServerStopsReplying() throws Exception {
        int port = fakeServer(1); // answers the hello, then goes silent
        RaftClient rc = client(port);
        assertEquals(1, rc.connect());

        assertTimeoutPreemptively(TEST_LIMIT, () -> {
            assertThrows(SocketTimeoutException.class, () -> rc.get(0, 1));
        });
    }

    @Test
    void redirectLoopGivesUp() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        toClose.add(ss);
        int port = ss.getLocalPort();
        // every hello is redirected back to this same server
        serve(ss, (s, in) -> {
            FrameCodec.readFrame(in);
            Response redirect = new Response(0, 2, new Redirect("localhost", port).toBytes());
            FrameCodec.writeFrame(s.getOutputStream(), redirect.serialize());
            FrameCodec.readFrame(in); // until the client closes
        });
        RaftClient rc = client(port);

        assertTimeoutPreemptively(TEST_LIMIT, () -> {
            IOException e = assertThrows(IOException.class, rc::connect);
            assertEquals("Gave up after " + RaftClient.MAX_REDIRECTS + " redirects, last to localhost:" + port,
                    e.getMessage());
        });
    }

    private RaftClient client(int port) {
        RaftClient rc = new RaftClient("localhost", port, RESPONSE_TIMEOUT);
        toClose.add(rc);
        return rc;
    }

    /**
     * A server that answers the first {@code replies} frames on each connection with Response(1,0)
     * and then reads without ever answering.
     */
    private int fakeServer(int replies) throws IOException {
        ServerSocket ss = new ServerSocket(0);
        toClose.add(ss);
        serve(ss, (s, in) -> {
            for (int i = 0; i < replies; i++) {
                FrameCodec.readFrame(in);
                FrameCodec.writeFrame(s.getOutputStream(), new Response(1, 0).serialize());
            }
            while (true) {
                FrameCodec.readFrame(in);
            }
        });
        return ss.getLocalPort();
    }

    interface Behaviour {
        void run(Socket s, DataInputStream in) throws Exception;
    }

    private void serve(ServerSocket ss, Behaviour behaviour) {
        Thread.ofVirtual().start(() -> {
            while (!ss.isClosed()) {
                try {
                    Socket s = ss.accept();
                    toClose.add(s);
                    Thread.ofVirtual().start(() -> {
                        try {
                            behaviour.run(s, new DataInputStream(s.getInputStream()));
                        } catch (Exception e) {
                            // client went away; nothing to do
                        }
                    });
                } catch (IOException e) {
                    return; // server socket closed
                }
            }
        });
    }
}
