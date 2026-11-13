package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.Message;
import com.mj.distributed.model.Member;
import com.mj.distributed.tcp.Connection;
import com.mj.distributed.tcp.ConnectionListener;
import com.mj.distributed.tcp.TcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;


/**
 * Opens an outbound connection to another server and registers it as a peer.
 *
 * Created by Manoj Khangaonkar on 10/5/2016.
 */
public class PeerClient implements ConnectionListener {

    private static final Logger LOG = LoggerFactory.getLogger(PeerClient.class);

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 2000;

    private final PeerServer peerServer;
    private final String remoteHost;
    private final int remotePort;

    private Connection connection;

    public PeerClient(String host, int port, PeerServer p) {
        this.remoteHost = host;
        this.remotePort = port;
        this.peerServer = p;
    }

    /**
     * Single attempt. On success the peer is registered with the server before this returns.
     */
    public void connect() throws IOException {
        connection = TcpClient.connect(remoteHost, remotePort, CONNECT_TIMEOUT, this);
    }

    /**
     * Retries with exponential backoff until connected or the server is stopped.
     * Returns false if the server stopped first.
     */
    public boolean connectWithRetry() throws InterruptedException {
        long backoff = INITIAL_BACKOFF_MS;
        while (!peerServer.isStopped()) {
            try {
                connect();
                return true;
            } catch (IOException e) {
                LOG.info(peerServer.getServerId() + ": cannot connect to " + remoteHost + ":" + remotePort
                        + " (" + e.getMessage() + "), retrying in " + backoff + "ms");
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
            }
        }
        return false;
    }

    public void queueSendMessage(Message m) throws Exception {
        connection.send(m.serialize());
    }

    @Override
    public void onOpened(Connection c) {
        peerServer.addPeer(c, new ConnectionPeer(new Member(remoteHost, remotePort), c));
    }

    @Override
    public void onFrame(Connection c, ByteBuffer frame) {
        peerServer.onFrame(c, frame);
    }

    @Override
    public void onClosed(Connection c) {
        LOG.info(peerServer.getServerId() + ": connection to " + remoteHost + ":" + remotePort + " dropped");
        peerServer.removePeer(c);
    }
}
