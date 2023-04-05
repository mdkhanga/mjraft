package com.mj.distributed.message.handler;

import com.mj.distributed.message.RequestVoteMessage;
import com.mj.distributed.message.RequestVoteResponseMessage;
import com.mj.distributed.message.TestClientHelloResponse;
import com.mj.distributed.model.LogEntryWithIndex;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class RequestVoteHandler implements MessageHandler {

    private static Logger LOG  = LoggerFactory.getLogger(RequestVoteHandler.class);

    public void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer peerServer) throws Exception {

        RequestVoteMessage message = RequestVoteMessage.deserialize(readBuffer.rewind());

        LOG.info(peerServer.getServerId() + ":Received a request vote message from " + message.getCandidateHost() + ":"
                + message.getCandidatePort() + " for term: "+message.getTerm());

        int requestVoteTerm = message.getTerm() ;

        LogEntryWithIndex lastEntry = peerServer.getLastEntry();

        boolean vote ;


        if ( message.getLastLogIndex() < lastEntry.getIndex() || message.getLastLogTerm() < lastEntry.getTerm()) {
            vote = false ;
            LOG.info(peerServer.getServerId() + ": voted No because last Entry of candidate not current " +
                    " message lastlogIndex =" + message.getLastLogIndex() +
                    " server lastlogIndex ="+lastEntry.getIndex() +
                    " message lastLogTerm = " + message.getLastLogTerm() +
                    " server lastlogterm = " + message.getLastLogTerm());
        }
        else if (peerServer.isElectionInProgress() && requestVoteTerm <= peerServer.getCurrentElectionTerm()) {
            vote = false ;
            LOG.info(peerServer.getServerId() + ": voted No because term < current " +
                    " request vote term =" + requestVoteTerm +
                    " election term="+peerServer.getCurrentElectionTerm());
        }
        else if (requestVoteTerm <= peerServer.getTerm()) {
            vote = false;
            LOG.info(peerServer.getServerId() + ": voted No because term < current term "+peerServer.getTerm());
        }
        else if (requestVoteTerm <= peerServer.getCurrentVotedTerm()) {
            vote = false ;
            LOG.info(peerServer.getServerId() + ": voted No because term < voted term "+peerServer.getCurrentVotedTerm());

        } else {

            peerServer.setElectionInProgress(message.getTerm(), null);
            // peerServer.setCurrentVotedTerm(message.getTerm());
            LOG.info(peerServer.getServerId() + ": voted Yes");
            vote = true ;
        }
        RequestVoteResponseMessage requestVoteResponseMessage = new RequestVoteResponseMessage(
                message.getTerm(),
                message.getCandidateHost(),
                message.getCandidatePort(),
                vote);

        peerServer.queueSendMessage(socketChannel, requestVoteResponseMessage);

    }
}
