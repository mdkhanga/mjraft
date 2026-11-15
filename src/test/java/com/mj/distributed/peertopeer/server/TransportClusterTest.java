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
        PeerServer[] cluster = Servers.startCluster(Ports.free(3));
        servers.addAll(List.of(cluster));
        PeerServer leader = Servers.awaitLeader(Duration.ofSeconds(30), cluster);

        // the NIO transport read into a fixed 8 KB buffer, so anything larger was corrupted
        byte[] value = new byte[20 * 1024];
        new Random(42).nextBytes(value);

        RaftClient raftClient = new RaftClient("localhost", leader.getBindPort());
        raftClient.connect();
        raftClient.send(value);

        Await.until(Duration.ofSeconds(20), "the 20 KB entry replicates to all servers",
                () -> Servers.allHaveLogSize(1, cluster));

        for (PeerServer s : cluster) {
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
        PeerServer second = start(new PeerServer(port));
        Servers.awaitLeader(Duration.ofSeconds(30), second); // a single server elects itself

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
