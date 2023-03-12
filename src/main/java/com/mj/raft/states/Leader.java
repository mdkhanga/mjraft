package com.mj.raft.states;

import com.mj.distributed.model.RaftState;
import com.mj.distributed.peertopeer.server.Peer;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Leader implements State, Runnable {


    private PeerServer server;
    private volatile boolean stop;

    private Logger LOG = LoggerFactory.getLogger(Leader.class);

    public Leader(PeerServer p) {
        server = p;
    }


    public void run() {

        LOG.info(server.getServerId()+ ":Entering leader state");

        AtomicInteger count = new AtomicInteger(1);

        while (!stop) {

            try {

                Thread.sleep(100);

                List<Peer> connectedPeers = server.getPeers() ;

                connectedPeers.forEach((v) -> {

                    try {

                        server.sendAppendEntriesMessage(v);

                        if (count.get() % 60 == 0) {
                            server.sendClusterInfoMessage(v);
                        }

                    } catch (Exception e) {
                        LOG.error("error", e);
                    }

                });

                count.incrementAndGet();

            } catch (Exception e) {
               LOG.error("Error in leader thread",e) ;
            }

        }

        LOG.info(server.getServerId() + ": exiting leader state.");

    }

    @Override
    public RaftState raftState() {
        return RaftState.leader;
    }

    @Override
    public void start() {
        server.startTask(this);
    }

    @Override
    public void stop() {
        LOG.info(server.getServerId()+":received req to stop leader state");
        stop = true;
    }

    @Override
    public void changeState(State newState) {
        server.setRaftState(newState);
        stop();
        newState.start();
    }


}
