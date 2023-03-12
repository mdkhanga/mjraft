package com.mj.distributed.message.handler;

import com.mj.distributed.message.AppendEntriesResponse;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class AppendEntriesResponseHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(AppendEntriesResponseHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {

        AppendEntriesResponse message = AppendEntriesResponse.deserialize(readBuffer.rewind());
        int index = message.getIndexAcked();


        if (index >= 0) {
            // LOG.info("got index for seqId " + message.getSeqOfMessageAcked()) ;
            peerServer.updateIndexAckCount(index);
        }


    }
}
