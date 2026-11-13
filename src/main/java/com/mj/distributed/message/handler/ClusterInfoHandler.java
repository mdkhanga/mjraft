package com.mj.distributed.message.handler;

import com.mj.distributed.message.ClusterInfoMessage;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import com.mj.distributed.tcp.Connection;

public class ClusterInfoHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(ClusterInfoHandler.class);

    public void handle(ByteBuffer readBuffer, Connection connection, PeerServer peerServer) throws Exception {

        ClusterInfoMessage message = ClusterInfoMessage.deserialize(readBuffer.rewind()) ;
        // LOG.info(peerServer.getServerId()+":Received clusterInfoMsg:" + message.toString());
        peerServer.setClusterInfo(message.getClusterInfo());

    }
}
