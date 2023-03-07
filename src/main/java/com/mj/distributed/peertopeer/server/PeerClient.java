package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.LogEntryWithIndex;
import com.mj.distributed.tcp.nio.NioCaller;
import com.mj.distributed.tcp.nio.NioCallerConsumer;
import com.mj.distributed.model.Member;
import org.slf4j.LoggerFactory ;
import org.slf4j.Logger ;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



/**
 * Created by Manoj Khangaonkar on 10/5/2016.
 */
public class PeerClient implements NioCallerConsumer {

    PeerServer peerServer;
    DataOutputStream dos;
    Integer peerServerId = -1;
    int remotePort; // port returned remote initiates the connection - other end of socket after our accept
    int remoteListenPort; // if we initate connection, this is where we connect to
    InetAddress remoteIpAddress;

    String remoteHost;

    ExecutorService peerClientExecutor = Executors.newFixedThreadPool(3);

    public volatile Queue<ByteBuffer> writeQueue = new ConcurrentLinkedDeque<ByteBuffer>();


    Logger LOG = LoggerFactory.getLogger(PeerClient.class);

    private NioCaller nioCaller;


    private PeerClient() {

    }

    public PeerClient(String host, int port, PeerServer p) {

        this.remoteHost = host;
        this.remotePort = port;
        this.peerServer = p;


    }


    public void start() throws Exception {

        nioCaller = new NioCaller(remoteHost, remotePort,
                peerServer.getBindHost(),
                peerServer.getBindPort(), this);
        nioCaller.start();

        // PeerClientStatusCallable peerClientStatusCallable = new PeerClientStatusCallable();
        // peerClientExecutor.submit(peerClientStatusCallable);

    }

    public boolean processLogEntry(LogEntryWithIndex e, int prevIndex, int prevTerm, int lastComittedIndex) throws Exception {

        return peerServer.processLogEntry(e, prevIndex, prevTerm, lastComittedIndex);
    }

    public void setLeaderHeartBeatTs(long ts) {
        peerServer.setLastLeaderHeartBeatTs(ts);
    }

    public void queueSendMessage(ByteBuffer b) {
        nioCaller.queueSendMessage(b);
    }

    public void addedConnection(SocketChannel s) {
        peerServer.addPeer(s, new CallerPeer(new Member(remoteHost, remotePort), this));
    }

    public void droppedConnection(SocketChannel s) {
        LOG.info(peerServer.getServerId() + ": a connection dropped");
        peerServer.removePeer(s);
    }

    public void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b) {
        peerServer.inBoundMessageCreator.submit(s, b, numBytes);
    }


    @Override
    public boolean equals(Object obj) {

        if (null == obj) {
            return false;
        }

        if (!(obj instanceof PeerClient)) {
            return false;
        }

        return peerServerId.equals(((PeerClient) obj).peerServerId);

    }

    @Override
    public int hashCode() {
        return peerServerId.hashCode();
    }

    public String getPeerServer() {
        return remoteIpAddress.toString() + ":" + remoteListenPort;
    }


}