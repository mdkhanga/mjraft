package com.mj.distributed.message.handler;

import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.message.Response;
import com.mj.distributed.message.TestClientHelloResponse;
import com.mj.distributed.model.Error;
import com.mj.distributed.model.Member;
import com.mj.distributed.model.Redirect;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TestHelloHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(TestHelloHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {

        LOG.info(peerServer.getServerId()+":Received a TestClient hello message");
        peerServer.addRaftClient(socketChannel);
        peerServer.queueSendMessage(socketChannel, new TestClientHelloResponse());

    }
}
