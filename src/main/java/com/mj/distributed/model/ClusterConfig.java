package com.mj.distributed.model;

import com.mj.distributed.utils.Utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * The fixed set of servers in the cluster, as configured at startup.
 *
 * Every server must be given the same members. Quorums are majorities of this set,
 * so a server that is down or unreachable still counts: losing connections never shrinks the majority.
 */
public record ClusterConfig(Member self, List<Member> members) {

    public ClusterConfig {
        members = List.copyOf(members);
        if (!members.contains(self)) {
            throw new IllegalArgumentException("Members " + members + " do not include this server " + self);
        }
        if (new HashSet<>(members).size() != members.size()) {
            throw new IllegalArgumentException("Duplicate members in " + members);
        }
    }

    /**
     * A one-server cluster. It elects itself, since it is its own majority.
     */
    public static ClusterConfig single(Member self) {
        return new ClusterConfig(self, List.of(self));
    }

    /**
     * @param spec comma separated host:port list, including this server, e.g. localhost:5001,localhost:5002
     */
    public static ClusterConfig parse(Member self, String spec) {
        List<Member> members = new ArrayList<>();
        for (String hostPort : spec.split(",")) {
            String[] parts = hostPort.trim().split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Expected host:port but got '" + hostPort + "'");
            }
            members.add(new Member(parts[0], Integer.parseInt(parts[1])));
        }
        return new ClusterConfig(self, members);
    }

    /**
     * Every member except this server.
     */
    public List<Member> others() {
        return members.stream().filter(m -> !m.equals(self)).toList();
    }

    public int size() {
        return members.size();
    }

    public int majority() {
        return Utils.majority(size());
    }
}
