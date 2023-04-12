package com.mj.distributed.message.handler;

import com.mj.distributed.message.RaftClientAppendEntry;
import com.mj.distributed.message.TestClientHelloResponse;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class RaftClientAppendEntryHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(RaftClientAppendEntryHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {

        LOG.info(peerServer.getServerId()+":Received a RaftClientAppendEntry message");
        RaftClientAppendEntry message = RaftClientAppendEntry.deserialize(readBuffer.rewind());
        peerServer.getRaftLog().addLogEntry(peerServer.getTerm(), message.getValue());

    }
}
