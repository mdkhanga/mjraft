package com.mj.distributed.message.handler;

import com.mj.distributed.message.GetServerLog;
import com.mj.distributed.message.GetServerLogResponse;
import com.mj.distributed.message.TestClientHelloResponse;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class GetServerLogHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(GetServerLogHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {
        LOG.info(peerServer.getServerId()+":Received a GetServerLog message");

        GetServerLog message = GetServerLog.deserialize(readBuffer.rewind());

        List<byte[]> ret = peerServer.getRaftLog().getLogEntries(message.getStartIndex(), message.getCount());

        GetServerLogResponse response = new GetServerLogResponse(message.getSeqId(), ret);

        LOG.info(peerServer.getServerId()+":Sending a GetServerLog response message");
        peerServer.queueSendMessage(socketChannel, response);


    }
}
