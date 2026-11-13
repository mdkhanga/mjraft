package com.mj.distributed.message.handler;

import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.message.Message;
import com.mj.distributed.message.Response;
import com.mj.distributed.model.Error;
import com.mj.distributed.model.Member;
import com.mj.distributed.model.Redirect;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.mj.distributed.tcp.Connection;

public class HelloHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(HelloHandler.class);

    public void handle(ByteBuffer readBuffer, Connection connection, PeerServer peerServer) throws Exception {

        HelloMessage message = HelloMessage.deserialize(readBuffer.rewind());
        peerServer.addPeer(connection, message.getHostString(), message.getHostPort());
        // LOG.info(peerServer.getServerId() + "Registered peer " + message.getHostString() + ":" + message.getHostPort());

        if (peerServer.isElectionInProgress()) {

            // LOG.info(peerServer.getServerId() + ":Election in progress return error");
            Error e = new Error(1, "Election in progress. Please wait.");
            peerServer.queueSendMessage(connection,
                    new Response(0, 1, e.toBytes()));

        } else if (!peerServer.isLeader()) {

            Member leader = peerServer.getLeader();
            LOG.info(peerServer.getServerId() + ":Redirecting to leader " + leader.getHostString() + ":" + leader.getPort());
            Redirect r = new Redirect(leader.getHostString(), leader.getPort());
            peerServer.queueSendMessage(connection,
                    new Response(0, 2, r.toBytes()));

        }

    }
}
