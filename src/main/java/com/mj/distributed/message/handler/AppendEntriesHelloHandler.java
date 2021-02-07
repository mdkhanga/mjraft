package com.mj.distributed.message.handler;

import com.mj.distributed.message.AppendEntriesResponse;
import com.mj.distributed.message.TestClientHelloResponse;
import com.mj.distributed.peertopeer.server.PeerData;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class AppendEntriesHelloHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(AppendEntriesHelloHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {

        AppendEntriesResponse message = AppendEntriesResponse.deserialize(readBuffer.rewind());
        PeerData d = peerServer.getPeerData(socketChannel);
        int index = d.getIndexAcked(message.getSeqOfMessageAcked());
        // LOG.info("Got AppendEntries response from" + d.getHostString() + "  " + d.getPort()) ;


        if (index >= 0) {
            // LOG.info("got index for seqId " + message.getSeqOfMessageAcked()) ;
            peerServer.updateIndexAckCount(index);
        }


    }
}
