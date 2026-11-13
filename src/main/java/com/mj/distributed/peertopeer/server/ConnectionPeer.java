package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.Message;
import com.mj.distributed.model.Member;
import com.mj.distributed.tcp.Connection;

/**
 * A peer reached over a Connection, whichever side opened it.
 * Replaces ListenerPeer (we accepted) and CallerPeer (we connected), which became identical.
 */
public class ConnectionPeer implements Peer {

    private final Member member ; // member we are connected to; null for raft clients
    private final Connection connection ;

    public ConnectionPeer(Member m, Connection c) {
        member = m ;
        connection = c ;
    }

    public void queueSendMessage(Message m) throws Exception {
        connection.send(m.serialize());
    }

    public void onReceiveMessage(Message m) {

    }

    public boolean active() {
        return connection.isOpen();
    }

    public Member member() {
        return member;
    }

    public void shutdown() {
        connection.close();
    }

    public Connection connection() {
        return connection;
    }
}
