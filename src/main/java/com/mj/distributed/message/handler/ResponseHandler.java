package com.mj.distributed.message.handler;

import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.message.Response;
import com.mj.distributed.model.Error;
import com.mj.distributed.model.Member;
import com.mj.distributed.model.Redirect;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ResponseHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(ResponseHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {

        Response r = Response.deserialize(readBuffer.rewind()) ;
        if (r.getStatus() == 0 && r.getType() == 1) {

            LOG.info("Election in progress. Trying again") ;
            // peerServer.stop() ;
            // TODO add delay
            HelloMessage m = new HelloMessage(peerServer.getBindHost(),
                    peerServer.getBindPort());
            peerServer.queueSendMessage(socketChannel, m);
        } else if (r.getStatus() == 0 && r.getType() == 2 ) {

            Redirect rd = Redirect.fromBytes(r.getDetails()) ;
            peerServer.redirect(socketChannel, rd);

        }

    }
}
