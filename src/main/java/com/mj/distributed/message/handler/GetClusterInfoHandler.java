package com.mj.distributed.message.handler;

import com.mj.distributed.message.ClusterInfoMessage;
import com.mj.distributed.message.TestClientHelloResponse;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class GetClusterInfoHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(GetClusterInfoHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {
        LOG.info(peerServer.getServerId()+":Received request for clusterInfo");
        ClusterInfoMessage cm = new ClusterInfoMessage(peerServer.getClusterInfo());
        // LOG.info(peerServer.getServerId()+":cm message size = "+cm.serialize().limit());
        peerServer.queueSendMessage(socketChannel, cm);
    }
}
