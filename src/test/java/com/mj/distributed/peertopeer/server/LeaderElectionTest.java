package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.ClusterInfo;
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


public class LeaderElectionTest {

    static PeerServer leader;
    static PeerServer server1;
    static PeerServer server2;


    @BeforeAll
    public static void init() throws Exception {
        System.out.println("running init") ;

        leader = new PeerServer(6001);
        leader.start() ;

        String[] seeds = new String[1];
        seeds[0] = "localhost:6001";

        server1 = new PeerServer(6002, seeds);
        server1.start();

        server2 = new PeerServer(6003, seeds);
        server2.start();


        Thread.sleep(10000);

        RaftClient raftClient = new RaftClient("localhost", 6001);
        raftClient.connect();

        raftClient.send(23);


        Thread.sleep(5000);

    }


    @AfterAll
    public static void destroy() throws Exception {

        server1.stop();
        server1 = null ;
        server2.stop();
        server2= null ;
    }

    @Test
    public void leaderElectionOnFailure() throws Exception {


        TestClient ts = new TestClient("localhost",6002);
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo() ;
        assertEquals(cs1.getLeader().getPort(),6001);
        assertEquals(cs1.getLeader().getHostString(),"localhost");


        TestClient ts2 = new TestClient("localhost",6003);
        ts2.connect();
        cs1 = ts2.getClusterInfo() ;
        assertEquals(cs1.getLeader().getPort(),6001);
        assertEquals(cs1.getLeader().getHostString(),"localhost");


        leader.stop();

        /* Thread.sleep(3000) ;

        RaftClient rs = new RaftClient("localhost", 6002);
        int ret = rs.connect() ;
        assertEquals(1, ret); */


        Thread.sleep(32000);

        // check for new leader
        cs1 = ts2.getClusterInfo() ;
        int port = cs1.getLeader().getPort();
        System.out.println("mj leader is "+ cs1.getLeader().getHostString() +":"+ port);

        if (port == 6001) {
            assertTrue(false);
        }


        ClusterInfo cs2 = ts.getClusterInfo();
        System.out.println("mj leader is "+cs2.getLeader().getHostString() +":"+ cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getPort(),cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(),cs2.getLeader().getHostString());

        // repeat
        Thread.sleep(1000) ;

        cs1 = ts2.getClusterInfo() ;
        port = cs1.getLeader().getPort();
        System.out.println("mj leader is "+ cs1.getLeader().getHostString() +":"+ port);

        if (port == 6001) {
            assertTrue(false);
        }


        cs2 = ts.getClusterInfo();
        System.out.println("mj leader is "+cs2.getLeader().getHostString() +":"+ cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getPort(),cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(),cs2.getLeader().getHostString());


        ts.close();
        ts2.close();

    }

    private List<Integer> convertToIntList(List<byte[]> bytes) {

        List<Integer> ret = new ArrayList<>();

        bytes.forEach(e->{
            ret.add(ByteBuffer.wrap(e).getInt());
        });

        return ret ;


    }
}