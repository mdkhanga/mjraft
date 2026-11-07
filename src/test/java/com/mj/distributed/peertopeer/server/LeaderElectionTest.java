package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.ClusterInfo;
import com.mj.raft.client.RaftClient;
import com.mj.raft.test.client.TestClient;
import com.mj.raft.testutil.Await;
import com.mj.raft.testutil.Ports;
import com.mj.raft.testutil.Servers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


@Tag("cluster")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class LeaderElectionTest {

    static int[] ports;
    static PeerServer leader;
    static PeerServer server1;
    static PeerServer server2;


    @BeforeAll
    public static void init() throws Exception {
        ports = Ports.free(3);

        leader = new PeerServer(ports[0]);
        leader.start() ;

        String[] seeds = { "localhost:" + ports[0] };

        server1 = new PeerServer(ports[1], seeds);
        server1.start();

        server2 = new PeerServer(ports[2], seeds);
        server2.start();

        // followers learn about each other only from the leader's periodic ClusterInfo gossip
        Await.until(Duration.ofSeconds(30), "all servers see the leader and know every member",
                () -> Servers.allSeeLeader(ports[0], server1, server2)
                        && Servers.allKnowMembers(3, leader, server1, server2));

        RaftClient raftClient = new RaftClient("localhost", ports[0]);
        raftClient.connect();

        raftClient.send(23);

        // voters reject candidates while their own log is empty, so the entry must replicate first
        Await.until(Duration.ofSeconds(20), "the entry replicates to all servers",
                () -> Servers.allHaveLogSize(1, leader, server1, server2));
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


        TestClient ts = new TestClient("localhost",ports[1]);
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo() ;
        assertEquals(ports[0], cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());


        TestClient ts2 = new TestClient("localhost",ports[2]);
        ts2.connect();
        cs1 = ts2.getClusterInfo() ;
        assertEquals(ports[0], cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());


        leader.stop();

        Await.until(Duration.ofSeconds(60), "survivors agree on a new leader",
                () -> Servers.leaderPort(server1) != ports[0]
                        && server1.getLeader().equals(server2.getLeader()));

        // check for new leader
        cs1 = ts2.getClusterInfo() ;
        assertNotEquals(ports[0], cs1.getLeader().getPort());

        ClusterInfo cs2 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(),cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(),cs2.getLeader().getHostString());

        // repeat after a pause to check the leader is stable
        Thread.sleep(1000) ;

        cs1 = ts2.getClusterInfo() ;
        assertNotEquals(ports[0], cs1.getLeader().getPort());

        cs2 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(),cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(),cs2.getLeader().getHostString());


        ts.close();
        ts2.close();

    }
}
