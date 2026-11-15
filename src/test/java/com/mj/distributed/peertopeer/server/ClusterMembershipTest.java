package com.mj.distributed.peertopeer.server;

import com.mj.raft.testutil.Await;
import com.mj.raft.testutil.Ports;
import com.mj.raft.testutil.Servers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The majority is fixed by configuration, so it does not shrink when servers go away.
 */
@Tag("cluster")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class ClusterMembershipTest {

    private PeerServer[] servers = new PeerServer[0];

    @AfterEach
    public void stopAll() throws Exception {
        Servers.stopAll(servers);
    }

    @Test
    public void firstLeaderIsElected() throws Exception {
        servers = Servers.startCluster(Ports.free(3));

        PeerServer leader = Servers.awaitLeader(Duration.ofSeconds(30), servers);

        assertTrue(leader.getTerm() >= 1, "the leader won an election, so its term is at least 1");
        Await.holds(Duration.ofSeconds(3), "the same leader stays in charge",
                () -> Servers.agreedLeader(servers).filter(l -> l == leader).isPresent());
    }

    /**
     * Stops the leader and enough followers to leave a minority. Before step 4 the survivors
     * shrank their member list as connections dropped, and a lone survivor elected itself.
     */
    @ParameterizedTest(name = "{0} servers")
    @ValueSource(ints = {3, 5})
    public void minorityCannotElectLeader(int clusterSize) throws Exception {
        servers = Servers.startCluster(Ports.free(clusterSize));
        PeerServer leader = Servers.awaitLeader(Duration.ofSeconds(30), servers);
        int termAtFailure = leader.getTerm();

        int toStop = clusterSize / 2 + 1; // a majority, leader included
        List<PeerServer> survivors = new ArrayList<>(Arrays.asList(servers));
        survivors.remove(leader);
        leader.stop();
        for (int i = 1; i < toStop; i++) {
            survivors.remove(0).stop();
        }
        PeerServer[] minority = survivors.toArray(new PeerServer[0]);
        assertEquals(clusterSize - toStop, minority.length);

        // long enough for several election timeouts
        Await.holds(Duration.ofSeconds(15), "no survivor becomes leader", () -> Servers.noneIsLeader(minority));

        for (PeerServer s : minority) {
            assertTrue(s.getCurrentElectionTerm() > termAtFailure,
                    s.getServerId() + " should have tried, and failed, to get elected");
        }
    }
}
