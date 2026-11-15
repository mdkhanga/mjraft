package com.mj.raft.testutil;

import com.mj.distributed.model.LogEntryWithIndex;
import com.mj.distributed.model.Member;
import com.mj.distributed.peertopeer.server.PeerServer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Starting test clusters, and read-only views of in-process server state used in wait conditions.
 */
public final class Servers {

    private Servers() {
    }

    public static List<Member> members(int... ports) {
        return Arrays.stream(ports).mapToObj(p -> new Member("localhost", p)).toList();
    }

    /**
     * Starts one server per port, each configured with all the ports as the cluster.
     */
    public static PeerServer[] startCluster(int... ports) throws Exception {
        List<Member> members = members(ports);
        PeerServer[] servers = new PeerServer[ports.length];
        for (int i = 0; i < ports.length; i++) {
            servers[i] = new PeerServer(ports[i], members);
            servers[i].start();
        }
        return servers;
    }

    /**
     * Waits until exactly one of the servers is leader and all the others follow it.
     */
    public static PeerServer awaitLeader(Duration timeout, PeerServer... servers) throws InterruptedException {
        Await.until(timeout, "the servers agree on one leader", () -> agreedLeader(servers).isPresent());
        return agreedLeader(servers).orElseThrow();
    }

    public static Optional<PeerServer> agreedLeader(PeerServer... servers) {
        List<PeerServer> leaders = Arrays.stream(servers).filter(PeerServer::isLeader).toList();
        if (leaders.size() != 1) {
            return Optional.empty();
        }
        PeerServer leader = leaders.get(0);
        boolean allFollow = Arrays.stream(servers).allMatch(s -> leaderPort(s) == leader.getBindPort());
        return allFollow ? Optional.of(leader) : Optional.empty();
    }

    public static boolean noneIsLeader(PeerServer... servers) {
        return Arrays.stream(servers).noneMatch(PeerServer::isLeader);
    }

    /**
     * @return the port of the leader s believes in, or -1 if it knows of none
     */
    public static int leaderPort(PeerServer s) {
        Member leader = s.getLeader();
        return leader == null ? -1 : leader.getPort();
    }

    public static int logSize(PeerServer s) {
        LogEntryWithIndex last = s.getRaftLog().getLastEntry();
        return last == null ? 0 : last.getIndex() + 1;
    }

    public static boolean allHaveLogSize(int size, PeerServer... servers) {
        return Arrays.stream(servers).allMatch(s -> logSize(s) == size);
    }

    public static void stopAll(PeerServer... servers) throws Exception {
        for (PeerServer s : servers) {
            s.stop();
        }
    }
}
