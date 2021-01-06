package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.model.RaftState;
import com.mj.raft.client.RaftClient;
import com.mj.raft.test.client.TestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LeaderElection5ServerTest {

    static PeerServer server1;
    static PeerServer server2;
    static PeerServer server3;
    static PeerServer server4;
    static PeerServer server5;

    @BeforeAll
    public static void init() throws Exception {
        System.out.println("running init") ;

        server1 = new PeerServer(7001);
        server1.start() ;

        String[] seeds = new String[1];
        seeds[0] = "localhost:7001";

        server2 = new PeerServer(2, 7002, seeds);
        server2.start();

        server3 = new PeerServer(3, 7003, seeds);
        server3.start();

        server4 = new PeerServer(4, 7004, seeds);
        server4.start();

        server5 = new PeerServer(5, 7005, seeds);
        server5.start();

        Thread.sleep(10000);

        RaftClient raftClient = new RaftClient("localhost", 7001);
        raftClient.connect();

        raftClient.send(23);


        Thread.sleep(5000);

    }


    @AfterAll
    public static void destroy() throws Exception {

        server3.stop();
        server3= null ;
        server4.stop();
        server4= null ;
        server5.stop();
        server5= null ;
    }

    @Test
    public void leaderElectionOn2Failure() throws Exception {


        TestClient ts = new TestClient("localhost",7002);
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo() ;
        assertEquals(cs1.getLeader().getPort(),7001);
        assertEquals(cs1.getLeader().getHostString(),"localhost");


        TestClient ts2 = new TestClient("localhost",7003);
        ts2.connect();
        cs1 = ts2.getClusterInfo() ;
        assertEquals(cs1.getLeader().getPort(),7001);
        assertEquals(cs1.getLeader().getHostString(),"localhost");

        System.out.println("Stopping server1") ;
        server1.stop();
        Thread.sleep(30000);

        // check for new leader
        cs1 = ts2.getClusterInfo() ;
        int port = cs1.getLeader().getPort();
        System.out.println("mj leader is "+ cs1.getLeader().getHostString() +":"+ port);

        if (port == 7001) {
            assertTrue(false);
        }


        ClusterInfo cs2 = ts.getClusterInfo();
        System.out.println("mj leader is "+cs2.getLeader().getHostString() +":"+ cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getPort(),cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(),cs2.getLeader().getHostString());

        // repeat
        Thread.sleep(5000) ;

        cs1 = ts2.getClusterInfo() ;
        port = cs1.getLeader().getPort();
        System.out.println("mj leader is "+ cs1.getLeader().getHostString() +":"+ port);

        if (port == 7001) {
            assertTrue(false);
        }


        cs2 = ts.getClusterInfo();
        System.out.println("mj leader is "+cs2.getLeader().getHostString() +":"+ cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getPort(),cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(),cs2.getLeader().getHostString());

        System.out.println("Stopping server2") ;
        server2.stop();
        Thread.sleep(30000);

        TestClient ts4 = new TestClient("localhost",7004);
        ts4.connect();
        ClusterInfo cs4 = ts2.getClusterInfo() ;
        System.out.println("mj new leader is "+cs4.getLeader().getHostString() +":"+ cs4.getLeader().getPort());

        TestClient ts5 = new TestClient("localhost",7005);
        ts5.connect();
        ClusterInfo cs5 = ts2.getClusterInfo() ;
        System.out.println("mj new leader is "+cs5.getLeader().getHostString() +":"+ cs5.getLeader().getPort());

        assertEquals(cs4.getLeader().getPort(),cs5.getLeader().getPort());
        assertEquals(cs4.getLeader().getHostString(),cs4.getLeader().getHostString());


        ts.close();
        ts2.close();
        ts4.close();
        ts5.close();

    }

    private List<Integer> convertToIntList(List<byte[]> bytes) {

        List<Integer> ret = new ArrayList<>();

        bytes.forEach(e->{
            ret.add(ByteBuffer.wrap(e).getInt());
        });

        return ret ;


    }
}