package com.mj.raft.testutil;

import com.mj.distributed.model.LogEntryWithIndex;
import com.mj.distributed.peertopeer.server.PeerServer;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Read-only views of in-process server state used in wait conditions.
 */
public final class Servers {

    private Servers() {
    }

    public static int leaderPort(PeerServer s) {
        return s.getLeader().getPort();
    }

    public static int logSize(PeerServer s) {
        LogEntryWithIndex last = s.getRaftLog().getLastEntry();
        return last == null ? 0 : last.getIndex() + 1;
    }

    public static boolean allHaveLogSize(int size, PeerServer... servers) {
        return Arrays.stream(servers).allMatch(s -> logSize(s) == size);
    }

    public static boolean allSeeLeader(int leaderPort, PeerServer... servers) {
        return Arrays.stream(servers).allMatch(s -> leaderPort(s) == leaderPort);
    }

    /**
     * True when every server knows exactly count distinct members (including itself).
     * Reads via getClusterInfo(), which copies under the members lock; the HashSet
     * drops the duplicate leader entry that ClusterInfo's constructor adds.
     */
    public static boolean allKnowMembers(int count, PeerServer... servers) {
        return Arrays.stream(servers)
                .allMatch(s -> new HashSet<>(s.getClusterInfo().getMembers()).size() == count);
    }
}
