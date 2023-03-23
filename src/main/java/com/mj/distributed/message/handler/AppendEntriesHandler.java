package com.mj.distributed.message.handler;

import com.mj.distributed.message.AppendEntriesMessage;
import com.mj.distributed.message.AppendEntriesResponse;
import com.mj.distributed.model.LogEntryWithIndex;
import com.mj.distributed.peertopeer.server.PeerData;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class AppendEntriesHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(AppendEntriesHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {

        AppendEntriesMessage message = AppendEntriesMessage.deserialize(readBuffer.rewind());
        PeerData d = peerServer.getPeerData(socketChannel);

        // AppendRPC rule 1 5.1 Page 4
        if (message.getTerm() < peerServer.getTerm()) {
            AppendEntriesResponse resp = new AppendEntriesResponse(-1, peerServer.getTerm(), false);
            ByteBuffer b = resp.serialize();
            peerServer.queueSendMessage(socketChannel, resp);
        }

        if ( !message.getLeaderId().equals(peerServer.getLeaderId()) ||
                message.getTerm() > peerServer.getTerm()) {
            LOG.info(peerServer.getServerId()+ ":We have a new leader :" + message.getLeaderId());
            peerServer.setLeader(message.getLeaderId());
            peerServer.setTerm(message.getTerm());
            if (peerServer.isElectionInProgress()) {
                LOG.info(peerServer.getServerId()+ " stopping leader election due to heartbeat from leader");
                peerServer.clearElectionInProgress();
            }
        }

        // LOG.info("Got append entries message "+ message.getLeaderId() + " " + d.getHostString() + " " + d.getPort());
        peerServer.setLastLeaderHeartBeatTs(System.currentTimeMillis());
        boolean entryResult = true ;
        LogEntryWithIndex e = message.getLogEntry() ;

        // rule 2 return false if prev entry does not match
        entryResult = peerServer.processLogEntry(e,message.getPrevIndex(), message.getTerm(), message.getLeaderCommitIndex()) ;

        int index = -1;
        if (e != null) {
            index = e.getIndex();
        }

        AppendEntriesResponse resp = new AppendEntriesResponse(index, peerServer.getTerm(), entryResult);
        ByteBuffer b = resp.serialize();
        peerServer.queueSendMessage(socketChannel, resp);

    }
}
