package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.MessageType;
import com.mj.distributed.message.RequestVoteMessage;
import com.mj.distributed.message.RequestVoteResponseMessage;
import com.mj.distributed.model.Member;
import com.mj.distributed.tcp.BlockingConnection;
import com.mj.raft.client.RaftClient;
import com.mj.raft.testutil.Await;
import com.mj.raft.testutil.Ports;
import com.mj.raft.testutil.Servers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Term, vote and log survive a restart. Each server keeps its state in its own directory under root.
 */
@Tag("cluster")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class RestartTest {

    private static final List<Integer> VALUES = List.of(23, 33, 44);

    @TempDir
    Path root;

    private final List<PeerServer> servers = new ArrayList<>();

    @AfterEach
    public void stopAll() throws Exception {
        for (PeerServer s : servers) {
            s.stop();
        }
    }

    @Test
    public void followerRestartKeepsItsLog() throws Exception {
        int[] ports = Ports.free(3);
        PeerServer[] cluster = track(Servers.startCluster(root, ports));
        PeerServer leader = Servers.awaitLeader(Duration.ofSeconds(30), cluster);
        send(leader, VALUES);
        Await.until(Duration.ofSeconds(20), "all servers have 3 log entries", () -> Servers.allHaveLogSize(3, cluster));

        PeerServer follower = Arrays.stream(cluster).filter(s -> s != leader).findFirst().orElseThrow();
        follower.stop();
        PeerServer restarted = track(Servers.restart(root, follower.getBindPort(), Servers.members(ports)))[0];

        // loaded by start(), so present before the leader sends anything
        assertEquals(VALUES, logValues(restarted));
    }

    @Test
    public void wholeClusterRestartKeepsEveryEntry() throws Exception {
        int[] ports = Ports.free(3);
        PeerServer[] cluster = track(Servers.startCluster(root, ports));
        PeerServer leader = Servers.awaitLeader(Duration.ofSeconds(30), cluster);
        send(leader, VALUES);
        Await.until(Duration.ofSeconds(20), "all servers have 3 log entries", () -> Servers.allHaveLogSize(3, cluster));
        int termBeforeRestart = Arrays.stream(cluster).mapToInt(PeerServer::getTerm).max().orElseThrow();

        Servers.stopAll(cluster);
        PeerServer[] again = track(Servers.startCluster(root, ports));

        for (PeerServer s : again) {
            assertEquals(VALUES, logValues(s), "log of " + s.getServerId());
        }
        PeerServer newLeader = Servers.awaitLeader(Duration.ofSeconds(30), again);
        // without a saved term, the restarted cluster would start counting from term 1 again
        assertTrue(newLeader.getTerm() > termBeforeRestart,
                "new term " + newLeader.getTerm() + " should be after " + termBeforeRestart);
    }

    @Test
    public void voteIsNotGivenTwiceInATermAcrossRestart() throws Exception {
        int port = Ports.free(1)[0];
        List<Member> members = Servers.members(port);
        PeerServer server = track(Servers.restart(root, port, members))[0];
        Servers.awaitLeader(Duration.ofSeconds(30), server);

        assertTrue(requestVote(port, 100, Ports.free(1)[0]), "the first candidate in term 100 gets the vote");

        server.stop();
        track(Servers.restart(root, port, members));

        assertFalse(requestVote(port, 100, Ports.free(1)[0]), "a second candidate in term 100 must not get a vote");
    }

    private PeerServer[] track(PeerServer... started) {
        servers.addAll(List.of(started));
        return started;
    }

    private static void send(PeerServer leader, List<Integer> values) throws Exception {
        try (RaftClient client = new RaftClient("localhost", leader.getBindPort())) {
            client.connect();
            for (int v : values) {
                client.send(v);
            }
        }
    }

    private static List<Integer> logValues(PeerServer s) {
        return s.getRaftLog().getLogEntries(0, Servers.logSize(s)).stream()
                .map(b -> ByteBuffer.wrap(b).getInt())
                .toList();
    }

    /**
     * Asks the server for its vote as a candidate that does not exist, on candidatePort.
     */
    private static boolean requestVote(int port, int term, int candidatePort) throws Exception {
        try (BlockingConnection c = BlockingConnection.open("localhost", port, Duration.ofSeconds(5), Duration.ofSeconds(10))) {
            RequestVoteMessage request = new RequestVoteMessage(term, "localhost", candidatePort, -1, -1);
            ByteBuffer reply = c.call(request, MessageType.RequestVoteResponse);
            return RequestVoteResponseMessage.deserialize(reply).getVote();
        }
    }
}
