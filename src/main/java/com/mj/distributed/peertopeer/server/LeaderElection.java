package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.message.RequestVoteMessage;
import com.mj.distributed.message.RequestVoteResponseMessage;
import com.mj.distributed.model.Member;
import com.mj.distributed.model.RaftState;
import com.mj.distributed.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderElection implements Runnable {

    private PeerServer server;

    private long electionStartTime ;
    private Logger LOG = LoggerFactory.getLogger(LeaderElection.class);
    private int requiredVotes ;
    private volatile AtomicInteger noVotes = new AtomicInteger(0);
    volatile AtomicInteger currentVoteCount = new AtomicInteger(1);
    private int newterm ;
    private List<Member> members;


    private volatile boolean stop = false;

    LeaderElection(PeerServer p) {
        server = p ;
        // int numServers = p.getClusterInfo().getMembers().size() - 1;
        members = p.getMembers();
        requiredVotes = Utils.majority(members.size()) ;
    }

    public static int ELECTION_TIMEOUT = 5000; // ms

    public void run()  {

        if (server.isElectionInProgress()) {
            LOG.info(server.getServerId()+ ":Election already in progress") ;
            return;
        } else {
            newterm = server.getNextElectionTerm();
            server.setElectionInProgress(newterm);
        }

        // start election timer
        electionStartTime = System.currentTimeMillis() ;

        LOG.info(server.getServerId()+ ":Started leader election at "+ electionStartTime + " for term "+ newterm);


        // get the list of available servers
        // List<Member> members = server.getClusterInfo().getMembers();

        synchronized (members) {
            members.forEach((m) -> {
                try {

                    if (m.isLeader()) {
                        LOG.info(server.getServerId() +":skipping "+m.getHostString() + ":" + m.getPort()) ;
                        return ;
                    }

                    if (m.getHostString().equals(server.getBindHost()) && m.getPort() == server.getBindPort()) {
                        LOG.info(server.getServerId() + ":skipping self") ;
                        return ;
                    }

                    /* if (m.getPort() == 5002) {
                        return ;
                    } */

                    LOG.info(server.getServerId() +":Sending request vote message to "+m.getHostString()+":"+m.getPort()) ;

                    /* PeerClient pc = new PeerClient(m.getHostString(), m.getPort(), server);
                    pc.start();
                    HelloMessage hm = new HelloMessage(server.getBindHost(), server.getBindPort());
                    pc.queueSendMessage(hm.serialize()); */

                    Peer pc = server.getPeer(m);

                    RequestVoteMessage rv = new RequestVoteMessage(
                            newterm,
                            server.getBindHost(),
                            server.getBindPort(),
                            server.getLastCommittedEntry());
                    // pc.queueSendMessage(rv.serialize());
                    pc.queueSendMessage(rv);
                } catch (Exception e) {
                    LOG.error("Error starting client in leader election", e);
                }

            });
        }

        while(!stop) {

            // LOG.info(server.getServerId() + ": election thread :" +newterm + "vote count =" +currentVoteCount.get()
               //     +": required = "+requiredVotes );

            if (currentVoteCount.get() >= requiredVotes) {

                LOG.info(server.getServerId()+":Won Election for term " + newterm + " with votes " + currentVoteCount.get()) ;
                server.setRaftState(RaftState.leader);
                server.setTerm(newterm);
                server.clearElectionInProgress();
                break;
            }

            if (noVotes.get() >= requiredVotes) {
                server.clearElectionInProgress();
                server.setRaftState(RaftState.follower);
                LOG.info(server.getServerId()+":Election vote NO  for term " + newterm) ;
                break;
            }

            long currentTime = System.currentTimeMillis() ;
            if (currentTime - electionStartTime > ELECTION_TIMEOUT) {
                server.clearElectionInProgress();
                server.setRaftState(RaftState.follower);
                LOG.info(server.getServerId()+":Election timed out for term " + newterm) ;
                break ;
            }

            try {
                Thread.sleep(200);
            } catch(Exception e) {
                LOG.error("Error woken from sleep",e);
            }
        }

        LOG.info("Election over for term " + newterm) ;
        // while( election not timed out && some else did not become leader)

            // if (checkVoteCount > majority)

                // done we

                // become leader and start sending heartbeat


                // exit



    }

    public void stop() {
        stop = true ;
    }

    public int vote(boolean v) {

        if (v) {
            return currentVoteCount.incrementAndGet();
        } else {
            noVotes.incrementAndGet();
            return currentVoteCount.get() ;
        }
    }

    public void requestVote(RequestVoteMessage message) {

        // if term < currentTerm
        // response = false

        // if votedFor == null or candidate Id

                //  candidate lastLogIndex == this.lastlogIndex and candidate.lastEntry = this.lastEntry

                // or

                // candiated.lastLogIndex > this.lastLogIndex

                // response = true
                // setVotedFor = candidate

        // response to candiate

    }

    public void requestVoteResponse(RequestVoteResponseMessage message) {

        // if term == election term and votedforId = this
        // increment vote count
    }



}
