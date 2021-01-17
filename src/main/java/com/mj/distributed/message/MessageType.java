package com.mj.distributed.message;

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
    Error(18);

    private int value ;

    MessageType(int n) {
        value = n;
    }

    public int value() {
        return value;
    }
}
