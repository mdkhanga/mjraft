package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.ClusterInfo;
import com.mj.raft.client.RaftClient;
import com.mj.raft.test.client.TestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LeaderElection5ServerTest {

    static PeerServer server1;
    static PeerServer server2;
    static PeerServer server3;
    static PeerServer server4;
    static PeerServer server5;

    static Set<Integer> alive = new HashSet<>() ;

    @BeforeAll
    public static void init() throws Exception {
        System.out.println("running init");

        server1 = new PeerServer(7001);
        server1.start();
        alive.add(7001);

        String[] seeds = new String[1];
        seeds[0] = "localhost:7001";

        server2 = new PeerServer(7002, seeds);
        server2.start();
        alive.add(7002);

        server3 = new PeerServer(7003, seeds);
        server3.start();
        alive.add(7003);

        server4 = new PeerServer(7004, seeds);
        server4.start();
        alive.add(7004);

        server5 = new PeerServer(7005, seeds);
        server5.start();
        alive.add(7005);

        Thread.sleep(10000);

        RaftClient raftClient = new RaftClient("localhost", 7001);
        raftClient.connect();

        raftClient.send(23);


        Thread.sleep(5000);

    }


    @AfterAll
    public static void destroy() throws Exception {

        /*server3.stop();
        server3 = null;
        server4.stop();
        server4 = null;
        server5.stop();
        server5 = null;*/
        alive.forEach(i-> {
            try {
                PeerServer p = getServerForLeader(i);
                p.stop();
            } catch(Exception e) {
                System.out.println(e);
            }
        });
    }

    @Test
    public void leaderElectionOn2Failure() throws Exception {


        TestClient ts = new TestClient("localhost", 7002);
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(), 7001);
        assertEquals(cs1.getLeader().getHostString(), "localhost");


        TestClient ts2 = new TestClient("localhost", 7003);
        ts2.connect();
        cs1 = ts2.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(), 7001);
        assertEquals(cs1.getLeader().getHostString(), "localhost");

        System.out.println("Stopping server1");
        server1.stop();
        alive.remove(7001);
        Thread.sleep(35000);

        // check for new leader
        System.out.println("Calling getClusterInfo 7003") ;
        cs1 = ts2.getClusterInfo();
        System.out.println("returned getClusterInfo 7003") ;
        int port = cs1.getLeader().getPort();
        System.out.println("mj leader is " + cs1.getLeader().getHostString() + ":" + port);

        if (port == 7001) {
            assertTrue(false);
        }


        System.out.println("Calling getClusterInfo 7002") ;
        ClusterInfo cs2 = ts.getClusterInfo();
        System.out.println("returned getClusterInfo 7002") ;
        System.out.println("mj leader is " + cs2.getLeader().getHostString() + ":" + cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getPort(), cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(), cs2.getLeader().getHostString());

        // repeat
        Thread.sleep(5000);

        System.out.println("2 Calling getClusterInfo 7003") ;
        cs1 = ts2.getClusterInfo();
        System.out.println("2 returned getClusterInfo 7003") ;
        port = cs1.getLeader().getPort();
        System.out.println("mj leader is " + cs1.getLeader().getHostString() + ":" + port);

        if (port == 7001) {
            assertTrue(false);
        }

        System.out.println("2 Calling getClusterInfo 7002") ;
        cs2 = ts.getClusterInfo();
        System.out.println("2 returned getClusterInfo 7002") ;
        System.out.println("mj leader is " + cs2.getLeader().getHostString() + ":" + cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getPort(), cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(), cs2.getLeader().getHostString());

        System.out.println("Stopping server "+cs2.getLeader().getHostString() + ":" + cs2.getLeader().getPort());
        PeerServer newleader = getServerForLeader(cs2.getLeader().getPort()) ;
        newleader.stop();
        alive.remove(cs2.getLeader().getPort());
        // server2.stop();
        Thread.sleep(35000);

       List<Integer> aliveList = new ArrayList<>(alive);

       System.out.println("ts4 trying to connect to " + aliveList.get(0)) ;
        TestClient ts4 = new TestClient("localhost", aliveList.get(0));
        ts4.connect();
        System.out.println("ts4 connected to " + aliveList.get(0)) ;
        ClusterInfo cs4 = ts4.getClusterInfo();
        System.out.println("1 mj new leader is " + cs4.getLeader().getHostString() + ":" + cs4.getLeader().getPort());

        System.out.println("ts5 trying to connect to " + aliveList.get(1)) ;
        TestClient ts5 = new TestClient("localhost", aliveList.get(1));
        ts5.connect();
        System.out.println("ts5 connected to " + aliveList.get(1)) ;
        ClusterInfo cs5 = ts5.getClusterInfo();
        System.out.println("2 mj new leader is " + cs5.getLeader().getHostString() + ":" + cs5.getLeader().getPort());

        assertEquals(cs4.getLeader().getPort(), cs5.getLeader().getPort());
        assertEquals(cs4.getLeader().getHostString(), cs4.getLeader().getHostString());

        System.out.println("Done with test");


        ts.close();
        ts2.close();
        ts4.close();
        ts5.close();

        System.out.println("Done calling close");
    }

    private static PeerServer getServerForLeader(int port) {

        if (port == 7002)
            return server2 ;
        else if (port == 7003)
            return server3 ;
        else if (port == 7004)
            return server4;
        else if(port == 7005)
            return server5;
        else
            throw new RuntimeException("unknown server");
    }

}