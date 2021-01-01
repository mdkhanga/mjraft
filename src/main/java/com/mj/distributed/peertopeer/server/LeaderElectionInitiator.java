package com.mj.distributed.peertopeer.server;

public class LeaderElectionInitiator implements Runnable {

    PeerServer peerServer ;

    LeaderElectionInitiator(PeerServer p) {

        peerServer = p;

    }


    public void run()  {

        while(true) {

            try {
                Thread.sleep(300) ;
                if (System.currentTimeMillis() - peerServer.getlastLeaderHeartBeatts() > 500) {


                }


            } catch(Exception e) {

            }
        }

    }

}
