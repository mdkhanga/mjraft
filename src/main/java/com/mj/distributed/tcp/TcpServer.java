package com.mj.distributed.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Accepts connections on a virtual thread. Closing the server also closes every connection it accepted.
 */
public final class TcpServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpServer.class);

    private final ServerSocket serverSocket;
    private final ConnectionListener listener;
    private final Set<Connection> connections = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    // forwards to the caller's listener and keeps the connection set current
    private final ConnectionListener tracking = new ConnectionListener() {
        @Override
        public void onOpened(Connection c) {
            listener.onOpened(c);
        }

        @Override
        public void onFrame(Connection c, ByteBuffer frame) {
            listener.onFrame(c, frame);
        }

        @Override
        public void onClosed(Connection c) {
            connections.remove(c);
            listener.onClosed(c);
        }
    };

    private TcpServer(ServerSocket serverSocket, ConnectionListener listener) {
        this.serverSocket = serverSocket;
        this.listener = listener;
    }

    /**
     * Binds before returning, so a port conflict fails here rather than on a background thread. Port 0 picks a free port.
     */
    public static TcpServer start(String host, int port, ConnectionListener listener) throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(host, port));
        TcpServer server = new TcpServer(ss, listener);
        Thread.ofVirtual().name("tcp-accept-" + ss.getLocalPort()).start(server::acceptLoop);
        return server;
    }

    public int localPort() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (!serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                TcpConnection c = new TcpConnection(socket, tracking);
                connections.add(c);
                c.start();
                if (closed) {
                    // close() ran while we were accepting
                    c.close();
                }
            } catch (IOException e) {
                if (!closed) {
                    LOG.error("Error accepting connection on port " + localPort(), e);
                }
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOG.debug("Error closing server socket", e);
        }
        for (Connection c : connections) {
            c.close();
        }
    }
}
