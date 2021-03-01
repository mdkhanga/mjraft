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

        LOG.info(server.getServerId()+": Entering follower state");

        while(!stop) {

            try {

                Thread.sleep(200);

                int randomDelay = Utils.getRandomDelay();
                Thread.sleep(randomDelay);
                // LOG.info("Got random Delay " + randomDelay);

                long timeSinceLastLeadetBeat = System.currentTimeMillis() -
                        server.getlastLeaderHeartBeatts();

                if ((server.getlastLeaderHeartBeatts() > 0
                        &&
                        timeSinceLastLeadetBeat > Candidate.ELECTION_TIMEOUT)) {



                    LOG.info(server.getServerId() + ":We need a leader Election. No heartBeat in " + timeSinceLastLeadetBeat);
                    // Thread.sleep(randomDelay);
                    LOG.info(server.getServerId() + "Try to start election after delay " + randomDelay);

                    // raftState = RaftStates.candidate;
                    Candidate cd = new Candidate(server) ;
                    changeState(cd);
                    stop();
                }

            } catch(Exception e) {
                LOG.error("Error in follower thread",e) ;
            }

        }

        LOG.info(server.getServerId()+": Exiting follower state");
    }


    @Override
    public RaftState raftState() {
        return RaftState.follower;
    }

    @Override
    public void start() {
        server.startTask(this);
    }

    @Override
    public void stop() {
        stop = true;
    }

    @Override
    public void changeState(State newState) {
        server.setRaftState(newState.raftState());
        stop();
        newState.start();
    }

}
