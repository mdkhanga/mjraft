package com.mj.raft.client;

import com.mj.distributed.message.Response;
import com.mj.distributed.peertopeer.server.PeerServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RaftClientTest {

    static PeerServer leader;
    static PeerServer server1;
    static PeerServer server2;

    @BeforeAll
    public static void init() throws Exception {
        System.out.println("running init") ;

        leader = new PeerServer(8001);
        leader.start() ;

        String[] seeds = new String[1];
        seeds[0] = "localhost:8001";

        server1 = new PeerServer(8002, seeds);
        server1.start();

        server2 = new PeerServer(8003, seeds);
        server2.start();

        Thread.sleep(15000);

    }

    @AfterAll
    public static void destroy() throws Exception {
        leader.stop();
        server1.stop();
        server2.stop();
    }

    @Test
    public void ClientRedirect() throws Exception {

        RaftClient rc = new RaftClient("localhost",8002) ;

        int ret = rc.connect() ;

        assert(1 == ret) ;

    }

    @Test
    void _connectLeader() throws Exception {

        RaftClient rc = new RaftClient("localhost",8001) ;

        Response r = rc._connect("localhost",8001) ;

        assertEquals(1, r.getStatus());

    }

    @Test
    void _connectNotLeader() throws Exception {

        RaftClient rc = new RaftClient("localhost",8003) ;

        Response r = rc._connect("localhost",8003) ;

        assertEquals(0, r.getStatus());
        assertEquals(2, r.getType());

    }
}