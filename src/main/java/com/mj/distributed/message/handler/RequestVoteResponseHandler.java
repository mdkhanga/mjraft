package com.mj.distributed.message.handler;

import com.mj.distributed.message.RequestVoteResponseMessage;
import com.mj.distributed.message.TestClientHelloResponse;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import com.mj.distributed.tcp.Connection;

public class RequestVoteResponseHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(RequestVoteResponseHandler.class);

    public void handle(ByteBuffer readBuffer, Connection connection, PeerServer peerServer) throws Exception {

        LOG.info(peerServer.getServerId()+":Received RequestVoteResponse Message") ;
        RequestVoteResponseMessage message = RequestVoteResponseMessage.deserialize(readBuffer.rewind());

        int votes = 0;
        if (message.getVote()) {
            votes = peerServer.vote(true);
            LOG.info(peerServer.getServerId()+":Got vote. updated vote count = " +votes) ;
        } else {
            votes = peerServer.vote(false);
            LOG.info(peerServer.getServerId()+":Did not get vote. current vote count="+votes);
        }

    }
}
