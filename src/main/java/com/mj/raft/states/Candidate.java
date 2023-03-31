package com.mj.raft.states;

import com.mj.distributed.message.RequestVoteMessage;
import com.mj.distributed.message.RequestVoteResponseMessage;
import com.mj.distributed.model.LogEntryWithIndex;
import com.mj.distributed.model.Member;
import com.mj.distributed.model.RaftState;
import com.mj.distributed.peertopeer.server.Peer;
import com.mj.distributed.peertopeer.server.PeerServer;
import com.mj.distributed.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Candidate implements State, Runnable {

    private PeerServer server;

    private long electionStartTime ;
    private Logger LOG = LoggerFactory.getLogger(Candidate.class);
    private int requiredVotes ;
    private volatile AtomicInteger noVotes = new AtomicInteger(0);
    volatile AtomicInteger currentVoteCount = new AtomicInteger(1);
    private int newterm ;
    private List<Member> members;


    private volatile boolean stop = false;

    public Candidate(PeerServer p) {
        server = p ;
        members = p.getMembers();
        requiredVotes = Utils.majority(members.size()) ;

    }

    public static int ELECTION_TIMEOUT = 5000; // ms

    public void run()  {

        LOG.info(server.getServerId()+":Entering candidate state") ;

        if (server.isElectionInProgress()) {
            LOG.info(server.getServerId()+ ":Election already in progress") ;
            return;
        } else {
            newterm = server.getNextElectionTerm();
            server.setElectionInProgress(newterm, this);
        }

        // start election timer
        electionStartTime = System.currentTimeMillis() ;

        LOG.info(server.getServerId()+ ":Started leader election at "+ electionStartTime + " for term "+ newterm);

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


                    LOG.info(server.getServerId() +":Sending request vote message to "+m.getHostString()+":"+m.getPort()) ;


                    Peer pc = server.getPeer(m);

                    LogEntryWithIndex last = server.getLastEntry();
                    int lastIndex = last == null ? -1 : last.getIndex();
                    int lastTerm = last == null ? -1 : last.getTerm();


                    RequestVoteMessage rv = new RequestVoteMessage(
                            newterm,
                            server.getBindHost(),
                            server.getBindPort(),
                            lastIndex, lastTerm);
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
                // server.setRaftState(RaftState.leader);
                changeState(new Leader(server));
                server.setTerm(newterm);
                server.clearElectionInProgress();
                break;
            }

            if (noVotes.get() >= requiredVotes) {
                server.clearElectionInProgress();
                // server.setRaftState(RaftState.follower);
                changeState(new Follower(server));
                LOG.info(server.getServerId()+":Election vote NO  for term " + newterm) ;
                break;
            }

            long currentTime = System.currentTimeMillis() ;
            if (currentTime - electionStartTime > ELECTION_TIMEOUT) {
                server.clearElectionInProgress();
                // server.setRaftState(RaftState.follower);
                changeState(new Follower(server));
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
        LOG.info(server.getServerId()+":Exiting candidate state") ;

    }

    @Override
    public RaftState raftState() {
        return RaftState.candidate;
    }

    @Override
    public void start() {
        server.startTask(this);
    }

    @Override
    public void stop() {
        LOG.info(server.getServerId()+ " stopping leader election");
        stop = true ;
    }

    @Override
    public void changeState(State newState) {
        server.setRaftState(newState);
        stop();
        newState.start();
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
