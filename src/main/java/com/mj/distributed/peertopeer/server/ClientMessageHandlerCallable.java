package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.*;
import com.mj.distributed.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;

public class ClientMessageHandlerCallable implements Callable {

    SocketChannel socketChannel ;
    ByteBuffer readBuffer ;
    PeerClient peerClient ;

    Logger LOG  = LoggerFactory.getLogger(ClientMessageHandlerCallable.class) ;

    public ClientMessageHandlerCallable(PeerClient p, SocketChannel s , ByteBuffer b) {

        socketChannel = s ;
        readBuffer = b ;
        peerClient = p ;

    }

    public Void call() {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int messageType = readBuffer.getInt() ;

        // LOG.info("Received message type " + messageType) ;
        try {
            if (messageType == MessageType.Ping.value()) {

                PingMessage message = PingMessage.deserialize(readBuffer.rewind());

                LOG.info("Received ping message from " + message.getServerId() + " seq :" + message.getSeqId());

                AckMessage resp = new AckMessage(message.getSeqId());

                ByteBuffer b = resp.serialize();

                peerClient.queueSendMessage(b);
            } else if (messageType == MessageType.AppendEntries.value()) {

                AppendEntriesMessage message = AppendEntriesMessage.deserialize(readBuffer.rewind());
                // LOG.info("Received AppendEntries message from " + message.getLeaderId() + " seq :" + message.getSeqId());
                peerClient.setLeaderHeartBeatTs(System.currentTimeMillis());
                boolean entryResult = true ;
                LogEntry e = message.getLogEntry() ;

                entryResult = peerClient.processLogEntry(e,message.getPrevIndex(),message.getLeaderCommitIndex()) ;
                AppendEntriesResponse resp = new AppendEntriesResponse(message.getSeqId(), 1, entryResult);
                ByteBuffer b = resp.serialize();
                peerClient.queueSendMessage(b);
            } else if (messageType == MessageType.ClusterInfo.value()) {

                ClusterInfoMessage message = ClusterInfoMessage.deserialize(readBuffer.rewind()) ;
                LOG.info("Received clusterInfoMsg:" + message.toString());
                peerClient.peerServer.setClusterInfo(message.getClusterInfo());
            } else if (messageType == MessageType.RequestVoteResponse.value()) {

                LOG.info("Received RequestVoteResponse Message") ;
                RequestVoteResponseMessage message = RequestVoteResponseMessage.deserialize(readBuffer.rewind());

                if (message.getVote()) {
                    LOG.info("Got vote. Won the election") ;
                } else {
                    LOG.info("Did not get vote. Lost the election");
                }


            }

        } catch(Exception e) {
            LOG.error("Error processing message",e);
        }

        return null ;
    }
}
