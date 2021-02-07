package com.mj.distributed.message;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.StreamSupport.stream;

public enum MessageType {
    Hello(1),
    Ping(2),
    Ack(3),
    AppendEntries(4),
    AppendEntriesResponse(5),
    ClusterInfo(6),
    RequestVote(7),
    RequestVoteResponse(8),
    RaftClientHello(10),
    RaftClientAppendEntry(11),
    GetServerLog(12),
    GetServerLogResponse(13),
    TestClientHello(14),
    GetClusterInfo(15),
    TestClientHelloResponse(16),
    RaftClientHelloResponse(17),
    Error(18),
    Response(19)
    ;

    private int value ;

    private final static Map<Integer, MessageType> map =
            Arrays.stream(MessageType.values()).collect(Collectors.toMap(t->t.value(), t->t));

    MessageType(int n) {
        value = n;
    }

    public int value() {
        return value;
    }

    public static MessageType valueOf(int v) {
        return map.get(v);
    }
}
