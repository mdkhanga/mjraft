package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.*;
import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.Callable;

public class ServerMessageHandlerCallable implements Callable {

    SocketChannel socketChannel ;
    ByteBuffer readBuffer ;
    PeerServer peerServer;

    Logger LOG  = LoggerFactory.getLogger(ServerMessageHandlerCallable.class) ;

    public ServerMessageHandlerCallable(PeerServer p, SocketChannel s , ByteBuffer b) {

        peerServer = p ;
        socketChannel = s ;
        readBuffer = b ;

    }


    public Void call() {

        // WARNING : 11142020
        // MIGHT BREAK CODE
        // commented read because rewind in InBoundMessage Creator was commented
        int messagesize = readBuffer.getInt() ;
        int messageType = readBuffer.getInt() ;

        // LOG.info("Received message of size " + messagesize) ;
        // LOG.info("Received message type " + messageType) ;

        try {

            if (messageType == MessageType.Hello.value()) {

               // LOG.info(peerServer.getServerId()+ ":Received a hello message");
                HelloMessage message = HelloMessage.deserialize(readBuffer.rewind());
                peerServer.addPeer(socketChannel, message.getHostString(), message.getHostPort());
                LOG.info(peerServer.getServerId()+"Registered peer " + message.getHostString() + ":" + message.getHostPort());

                /* if (newMember && peerServer is not leader && noElection in progress) {

                    respond with redirect message ;
                } else if (newMember && electionin progress) {

                    respond with status message election in progress
                } */

            } else if(messageType == MessageType.TestClientHello.value()) {

                LOG.info(peerServer.getServerId()+":Received a TestClient hello message");
                peerServer.addRaftClient(socketChannel);
                peerServer.queueSendMessage(socketChannel, new TestClientHelloResponse());

            } else if (messageType == MessageType.Ack.value()) {

                AckMessage message = AckMessage.deserialize(readBuffer.rewind());

                // LOG.info("Received ack message from " + d.member().getHostString() + ":" + d.member().getPort() + " with seq " + message.getSeqOfMessageAcked());
            } else if (messageType == 5) {
                AppendEntriesResponse message = AppendEntriesResponse.deserialize(readBuffer.rewind());
                PeerData d = peerServer.getPeerData(socketChannel);
                int index = d.getIndexAcked(message.getSeqOfMessageAcked());
                // LOG.info("Got AppendEntries response from" + d.getHostString() + "  " + d.getPort()) ;


                if (index >= 0) {
                   // LOG.info("got index for seqId " + message.getSeqOfMessageAcked()) ;
                    peerServer.updateIndexAckCount(index);
                } else {
                    // LOG.info("Not updating ack count") ;
                }

                // LOG.info("Received an appendEntriesResponse message from " + d.getHostString() + ":" + d.getPort()
                //
                //+ " with seq " + message.getSeqOfMessageAcked());
            } else if (messageType == MessageType.RequestVote.value()) {

                RequestVoteMessage message = RequestVoteMessage.deserialize(readBuffer.rewind());

                LOG.info(peerServer.getServerId() + ":Received a request vote message from " + message.getCandidateHost() + ":"
                        + message.getCandidatePort() + " for term: "+message.getTerm());

                int requestVoteTerm = message.getTerm() ;

                boolean vote ;

                /* if (peerServer.isElectionInProgress() && requestVoteTerm <= peerServer.getCurrentElectionTerm()) {
                    vote = false ;
                    LOG.info(peerServer.getServerId() + ": voted No because term < current " +
                            "election term "+peerServer.getCurrentElectionTerm());
                }
                else */ if (requestVoteTerm <= peerServer.getTerm()) {
                    vote = false;
                    LOG.info(peerServer.getServerId() + ": voted No because term < current term "+peerServer.getTerm());
                } else if (peerServer.getRaftState() == RaftState.candidate &&
                        requestVoteTerm <= peerServer.getTerm()+1 ) {
                    vote = false;
                    LOG.info(peerServer.getServerId() + ": voted No because we are candidate");
                }
                else if (requestVoteTerm <= peerServer.getCurrentVotedTerm()) {
                    vote = false ;
                    LOG.info(peerServer.getServerId() + ": voted No because term < voted term "+peerServer.getCurrentVotedTerm());
                } else {

                    peerServer.setCurrentVotedTerm(message.getTerm());
                    LOG.info(peerServer.getServerId() + ": voted Yes");
                    vote = true ;
                }
                RequestVoteResponseMessage requestVoteResponseMessage = new RequestVoteResponseMessage(
                        message.getTerm(),
                        message.getCandidateHost(),
                        message.getCandidatePort(),
                        vote);

                peerServer.queueSendMessage(socketChannel, requestVoteResponseMessage);

            } else if (messageType == MessageType.AppendEntries.value()) {
                AppendEntriesMessage message = AppendEntriesMessage.deserialize(readBuffer.rewind());
                PeerData d = peerServer.getPeerData(socketChannel);

                if ( !message.getLeaderId().equals(peerServer.getLeaderId()) ||
                        message.getTerm() != peerServer.getTerm()) {
                    LOG.info(peerServer.getServerId()+ ":We have a new leader :" + message.getLeaderId());
                    peerServer.setLeader(message.getLeaderId());
                    peerServer.currentTerm.set(message.getTerm());
                    if (peerServer.isElectionInProgress()) {
                        peerServer.clearElectionInProgress();
                    }
                }


                // LOG.info("Got append entries message "+ message.getLeaderId() + " " + d.getHostString() + " " + d.getPort());
                peerServer.setLastLeaderHeartBeatTs(System.currentTimeMillis());
                boolean entryResult = true ;
                LogEntry e = message.getLogEntry() ;
                entryResult = peerServer.processLogEntry(e,message.getPrevIndex(),message.getLeaderCommitIndex()) ;
                AppendEntriesResponse resp = new AppendEntriesResponse(message.getSeqId(), 1, entryResult);
                ByteBuffer b = resp.serialize();
                peerServer.queueSendMessage(socketChannel, resp);
            }  else if (messageType == MessageType.ClusterInfo.value()) {

                ClusterInfoMessage message = ClusterInfoMessage.deserialize(readBuffer.rewind()) ;
                LOG.info(peerServer.getServerId()+":Received clusterInfoMsg:" + message.toString());
                peerServer.setClusterInfo(message.getClusterInfo());
            } else if (messageType == MessageType.RequestVoteResponse.value()) {

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

            } else if (messageType == MessageType.RaftClientHello.value()) {

                LOG.info(peerServer.getServerId()+":Received a RaftClientHello message") ;
                peerServer.addRaftClient(socketChannel);

                if (peerServer.isElectionInProgress()) {
                    LOG.info(peerServer.getServerId()+":Election in progress return error") ;
                    peerServer.queueSendMessage(socketChannel,
                            new ErrorResponse(1, "Election in progress. Please wait."));


                    return null ;
                }



                peerServer.queueSendMessage(socketChannel, new RaftClientHelloResponse());

            } else if (messageType == MessageType.RaftClientAppendEntry.value()) {

                LOG.info(peerServer.getServerId()+":Received a RaftClientAppendEntry message");
                RaftClientAppendEntry message = RaftClientAppendEntry.deserialize(readBuffer.rewind());
                peerServer.addLogEntry(message.getValue());

            } else if (messageType == MessageType.GetServerLog.value()) {

                LOG.info(peerServer.getServerId()+":Received a GetServerLog message");

                GetServerLog message = GetServerLog.deserialize(readBuffer.rewind());

                List<byte[]> ret = peerServer.getLogEntries(message.getStartIndex(), message.getCount());

                GetServerLogResponse response = new GetServerLogResponse(message.getSeqId(), ret);

                LOG.info(peerServer.getServerId()+":Sending a GetServerLog response message");
                peerServer.queueSendMessage(socketChannel, response);
            } else if (messageType == MessageType.GetClusterInfo.value()) {
                LOG.info(peerServer.getServerId()+":Received request for clusterInfo");
                ClusterInfoMessage cm = new ClusterInfoMessage(peerServer.getClusterInfo());
                LOG.info(peerServer.getServerId()+":cm message size = "+cm.serialize().limit());
                peerServer.queueSendMessage(socketChannel, cm);

            }
            else {
                LOG.info("Received message of unknown type " + messageType);
            }

        } catch(Exception e) {
            LOG.error("Error deserializing message ",e);
        }

        return null ;
    }
}
