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

        PeerServer first = new PeerServer(ports[0]);
        first.start();
        alive.put(ports[0], first);

        String[] seeds = { "localhost:" + ports[0] };

        for (int i = 1; i < ports.length; i++) {
            PeerServer s = new PeerServer(ports[i], seeds);
            s.start();
            alive.put(ports[i], s);
        }

        // followers learn about each other only from the leader's periodic ClusterInfo gossip
        Await.until(Duration.ofSeconds(30), "all servers see the leader and know every member",
                () -> Servers.allSeeLeader(ports[0], aliveServers())
                        && Servers.allKnowMembers(5, aliveServers()));

        RaftClient raftClient = new RaftClient("localhost", ports[0]);
        raftClient.connect();

        raftClient.send(23);

        // voters reject candidates while their own log is empty, so the entry must replicate first
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


        TestClient ts = new TestClient("localhost", ports[1]);
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo();
        assertEquals(ports[0], cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());


        TestClient ts2 = new TestClient("localhost", ports[2]);
        ts2.connect();
        cs1 = ts2.getClusterInfo();
        assertEquals(ports[0], cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());

        stop(ports[0]);

        PeerServer s1 = alive.get(ports[1]);
        PeerServer s2 = alive.get(ports[2]);
        Await.until(Duration.ofSeconds(60), "servers agree on a new leader after the first failure",
                () -> alive.containsKey(Servers.leaderPort(s1))
                        && s1.getLeader().equals(s2.getLeader()));

        // check for new leader
        cs1 = ts2.getClusterInfo();
        assertNotEquals(ports[0], cs1.getLeader().getPort());

        ClusterInfo cs2 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(), cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(), cs2.getLeader().getHostString());

        // repeat after a pause to check the leader is stable
        Thread.sleep(1000);

        cs1 = ts2.getClusterInfo();
        assertNotEquals(ports[0], cs1.getLeader().getPort());

        cs2 = ts.getClusterInfo();
        assertEquals(cs1.getLeader().getPort(), cs2.getLeader().getPort());
        assertEquals(cs1.getLeader().getHostString(), cs2.getLeader().getHostString());

        int secondLeader = cs2.getLeader().getPort();
        stop(secondLeader);

        List<Integer> aliveList = new ArrayList<>(alive.keySet());
        PeerServer a = alive.get(aliveList.get(0));
        PeerServer b = alive.get(aliveList.get(1));
        Await.until(Duration.ofSeconds(60), "servers agree on a new leader after the second failure",
                () -> alive.containsKey(Servers.leaderPort(a))
                        && a.getLeader().equals(b.getLeader()));

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
