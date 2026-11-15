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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Tag("cluster")
@Timeout(value = 4, unit = TimeUnit.MINUTES)
public class LeaderElection5ServerTest {

    static int[] ports;

    // port -> running server
    static final Map<Integer, PeerServer> alive = new LinkedHashMap<>();

    @BeforeAll
    public static void init() throws Exception {
        ports = Ports.free(5);

        for (PeerServer s : Servers.startCluster(ports)) {
            alive.put(s.getBindPort(), s);
        }

        PeerServer leader = Servers.awaitLeader(Duration.ofSeconds(30), aliveServers());

        RaftClient raftClient = new RaftClient("localhost", leader.getBindPort());
        raftClient.connect();

        raftClient.send(23);

        // the failover election then runs with a non-empty log on every server
        Await.until(Duration.ofSeconds(20), "the entry replicates to all servers",
                () -> Servers.allHaveLogSize(1, aliveServers()));
    }


    @AfterAll
    public static void destroy() throws Exception {
        for (PeerServer p : alive.values()) {
            try {
                p.stop();
            } catch(Exception e) {
                System.out.println(e);
            }
        }
    }

    @Test
    public void leaderElectionOn2Failure() throws Exception {


        int firstLeader = Servers.agreedLeader(aliveServers()).orElseThrow().getBindPort();
        List<Integer> followers = new ArrayList<>(alive.keySet());
        followers.remove(Integer.valueOf(firstLeader));

        TestClient ts = new TestClient("localhost", followers.get(0));
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo();
        assertEquals(firstLeader, cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());


        TestClient ts2 = new TestClient("localhost", followers.get(1));
        ts2.connect();
        cs1 = ts2.getClusterInfo();
        assertEquals(firstLeader, cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());

        stop(firstLeader);

        Servers.awaitLeader(Duration.ofSeconds(60), aliveServers());

        // check for new leader
        cs1 = ts2.getClusterInfo();
        assertNotEquals(firstLeader, cs1.getLeader().getPort());

        ClusterInfo cs2 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(), cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(), cs2.getLeader().getHostString());

        // repeat after a pause to check the leader is stable
        Thread.sleep(1000);

        cs1 = ts2.getClusterInfo();
        assertNotEquals(firstLeader, cs1.getLeader().getPort());

        cs2 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(), cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(), cs2.getLeader().getHostString());

        int secondLeader = cs2.getLeader().getPort();
        stop(secondLeader);

        // 3 of 5 left: still a majority of the configured cluster
        Servers.awaitLeader(Duration.ofSeconds(60), aliveServers());
        List<Integer> aliveList = new ArrayList<>(alive.keySet());

        TestClient ts4 = new TestClient("localhost", aliveList.get(0));
        ts4.connect();
        ClusterInfo cs4 = ts4.getClusterInfo();

        TestClient ts5 = new TestClient("localhost", aliveList.get(1));
        ts5.connect();
        ClusterInfo cs5 = ts5.getClusterInfo();

        assertTrue(alive.containsKey(cs4.getLeader().getPort()));
        assertEquals(cs4.getLeader().getPort(), cs5.getLeader().getPort());
        assertEquals(cs4.getLeader().getHostString(), cs5.getLeader().getHostString());


        ts.close();
        ts2.close();
        ts4.close();
        ts5.close();
    }

    private static void stop(int port) throws Exception {
        alive.remove(port).stop();
    }

    private static PeerServer[] aliveServers() {
        return alive.values().toArray(new PeerServer[0]);
    }

}
