package com.mj.raft.states;

import com.mj.distributed.model.RaftState;
import com.mj.distributed.peertopeer.server.PeerServer;
import com.mj.distributed.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Follower implements State, Runnable {

    private PeerServer server;
    private volatile boolean stop;

    private Logger LOG = LoggerFactory.getLogger(Follower.class);

    public Follower(PeerServer p) {

        server = p;
    }

    public void run() {

        while(!stop) {

            try {

                Thread.sleep(200);

                int randomDelay = Utils.getRandomDelay();
                // LOG.info("Got random Delay " + randomDelay);

                long timeSinceLastLeadetBeat = System.currentTimeMillis() -
                        server.getlastLeaderHeartBeatts();

                if ((server.getlastLeaderHeartBeatts() > 0 && timeSinceLastLeadetBeat > randomDelay)
                        &&
                        (System.currentTimeMillis() - server.getCurrentVoteTimeStamp() > Candidate.ELECTION_TIMEOUT)) {

                    LOG.info(server.getServerId() + ":We need a leader Election. No heartBeat in ");
                    // raftState = RaftStates.candidate;
                    changeState(new Candidate(server));
                }

            } catch(Exception e) {
                LOG.error("Error in follower thread",e) ;
            }

        }

    }


    @Override
    public RaftState raftState() {
        return RaftState.follower;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void changeState(State newState) {
        server.setRaftState(newState.raftState());
        stop();
        newState.start();
    }

}
