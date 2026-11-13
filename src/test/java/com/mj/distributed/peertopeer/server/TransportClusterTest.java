package com.mj.distributed.peertopeer.server;

import com.mj.raft.client.RaftClient;
import com.mj.raft.testutil.Await;
import com.mj.raft.testutil.Ports;
import com.mj.raft.testutil.Servers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Server-level checks of the socket transport: frame sizes, and that stop() releases resources.
 */
@Tag("cluster")
@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class TransportClusterTest {

    private final List<PeerServer> servers = new ArrayList<>();

    @AfterEach
    public void stopAll() throws Exception {
        for (PeerServer s : servers) {
            s.stop();
        }
    }

    @Test
    public void valueLargerThanOldReadBufferReplicates() throws Exception {
        int[] ports = Ports.free(3);
        PeerServer leader = start(new PeerServer(ports[0]));
        String[] seeds = { "localhost:" + ports[0] };
        PeerServer server1 = start(new PeerServer(ports[1], seeds));
        PeerServer server2 = start(new PeerServer(ports[2], seeds));

        Await.until(Duration.ofSeconds(20), "followers see the initial leader",
                () -> Servers.allSeeLeader(ports[0], server1, server2));

        // the NIO transport read into a fixed 8 KB buffer, so anything larger was corrupted
        byte[] value = new byte[20 * 1024];
        new Random(42).nextBytes(value);

        RaftClient raftClient = new RaftClient("localhost", ports[0]);
        raftClient.connect();
        raftClient.send(value);

        Await.until(Duration.ofSeconds(20), "the 20 KB entry replicates to all servers",
                () -> Servers.allHaveLogSize(1, leader, server1, server2));

        for (PeerServer s : List.of(leader, server1, server2)) {
            assertArrayEquals(value, s.getRaftLog().getLastEntry().getEntry(), "log entry on " + s.getServerId());
        }

        raftClient.close();
    }

    @Test
    public void stopReleasesThePort() throws Exception {
        int port = Ports.free(1)[0];

        PeerServer first = start(new PeerServer(port));
        first.stop();

        // binding would throw if stop() had left the listening socket open
        start(new PeerServer(port));

        RaftClient raftClient = new RaftClient("localhost", port);
        assertEquals(1, raftClient.connect());
        raftClient.close();
    }

    @Test
    public void stopCanBeCalledTwice() throws Exception {
        PeerServer s = start(new PeerServer(Ports.free(1)[0]));

        s.stop();
        s.stop();

        assertTrue(s.isStopped());
    }

    private PeerServer start(PeerServer s) throws Exception {
        servers.add(s);
        s.start();
        return s;
    }
}
