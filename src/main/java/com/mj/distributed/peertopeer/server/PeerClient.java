package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.Member;
import com.mj.distributed.tcp.Connection;
import com.mj.distributed.tcp.ConnectionListener;
import com.mj.distributed.tcp.TcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;


/**
 * Keeps an outbound connection to one other member open for the life of the server,
 * reconnecting with backoff whenever it drops. Messages to that member are sent on this connection;
 * its replies come back on it too.
 *
 * Created by Manoj Khangaonkar on 10/5/2016.
 */
public class PeerClient implements ConnectionListener {

    private static final Logger LOG = LoggerFactory.getLogger(PeerClient.class);

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 2000;

    private final PeerServer peerServer;
    private final Member member;

    // completed when the current connection closes
    private volatile CompletableFuture<Void> closed = new CompletableFuture<>();

    public PeerClient(Member member, PeerServer p) {
        this.member = member;
        this.peerServer = p;
    }

    /**
     * Starts connecting in the background. Returns immediately.
     */
    public void start() {
        Thread.ofVirtual().name("peer-client-" + member.getHostString() + ":" + member.getPort()).start(this::maintain);
    }

    private void maintain() {
        try {
            while (!peerServer.isStopped()) {
                if (!connectWithRetry()) {
                    return;
                }
                closed.join();
                Thread.sleep(INITIAL_BACKOFF_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Retries with exponential backoff until connected or the server is stopped.
     * Returns false if the server stopped first.
     */
    private boolean connectWithRetry() throws InterruptedException {
        long backoff = INITIAL_BACKOFF_MS;
        while (!peerServer.isStopped()) {
            closed = new CompletableFuture<>(); // before connecting: onClosed can run before connect returns
            try {
                TcpClient.connect(member.getHostString(), member.getPort(), CONNECT_TIMEOUT, this);
                return true;
            } catch (IOException e) {
                LOG.debug(peerServer.getServerId() + ": cannot connect to " + member
                        + " (" + e.getMessage() + "), retrying in " + backoff + "ms");
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
            }
        }
        return false;
    }

    @Override
    public void onOpened(Connection c) {
        LOG.info(peerServer.getServerId() + ": connected to " + member);
        peerServer.addPeer(c, new ConnectionPeer(member, c));
    }

    @Override
    public void onFrame(Connection c, ByteBuffer frame) {
        peerServer.onFrame(c, frame);
    }

    @Override
    public void onClosed(Connection c) {
        LOG.info(peerServer.getServerId() + ": connection to " + member + " dropped");
        peerServer.removePeer(c);
        closed.complete(null);
    }
}
