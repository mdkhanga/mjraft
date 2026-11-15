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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


@Tag("cluster")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class LeaderElectionTest {

    static int[] ports;
    static PeerServer[] servers;
    static PeerServer leader; // elected, so not necessarily ports[0]


    @BeforeAll
    public static void init() throws Exception {
        ports = Ports.free(3);
        servers = Servers.startCluster(ports);
        leader = Servers.awaitLeader(Duration.ofSeconds(30), servers);

        RaftClient raftClient = new RaftClient("localhost", leader.getBindPort());
        raftClient.connect();

        raftClient.send(23);

        // the failover election then runs with a non-empty log on every server
        Await.until(Duration.ofSeconds(20), "the entry replicates to all servers",
                () -> Servers.allHaveLogSize(1, servers));
    }


    @AfterAll
    public static void destroy() throws Exception {

        Servers.stopAll(servers);
    }

    @Test
    public void leaderElectionOnFailure() throws Exception {


        int oldLeader = leader.getBindPort();
        PeerServer[] survivors = Arrays.stream(servers).filter(s -> s != leader).toArray(PeerServer[]::new);

        TestClient ts = new TestClient("localhost", survivors[0].getBindPort());
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo() ;
        assertEquals(oldLeader, cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());


        TestClient ts2 = new TestClient("localhost", survivors[1].getBindPort());
        ts2.connect();
        cs1 = ts2.getClusterInfo() ;
        assertEquals(oldLeader, cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());


        leader.stop();

        Servers.awaitLeader(Duration.ofSeconds(60), survivors);

        // check for new leader
        cs1 = ts2.getClusterInfo() ;
        assertNotEquals(oldLeader, cs1.getLeader().getPort());

        ClusterInfo cs2 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(),cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(),cs2.getLeader().getHostString());

        // repeat after a pause to check the leader is stable
        Thread.sleep(1000) ;

        cs1 = ts2.getClusterInfo() ;
        assertNotEquals(oldLeader, cs1.getLeader().getPort());

        cs2 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(),cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(),cs2.getLeader().getHostString());


        ts.close();
        ts2.close();

    }
}
