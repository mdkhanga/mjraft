package com.mj.raft.client;

import com.mj.distributed.message.Response;
import com.mj.distributed.peertopeer.server.PeerServer;
import com.mj.raft.testutil.Await;
import com.mj.raft.testutil.Ports;
import com.mj.raft.testutil.Servers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("cluster")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class RaftClientTest {

    static int[] ports;
    static PeerServer leader;
    static PeerServer server1;
    static PeerServer server2;

    @BeforeAll
    public static void init() throws Exception {
        ports = Ports.free(3);

        leader = new PeerServer(ports[0]);
        leader.start() ;

        String[] seeds = { "localhost:" + ports[0] };

        server1 = new PeerServer(ports[1], seeds);
        server1.start();

        server2 = new PeerServer(ports[2], seeds);
        server2.start();

        Await.until(Duration.ofSeconds(20), "followers see the initial leader",
                () -> Servers.allSeeLeader(ports[0], server1, server2));
    }

    @AfterAll
    public static void destroy() throws Exception {
        leader.stop();
        server1.stop();
        server2.stop();
    }

    @Test
    public void ClientRedirect() throws Exception {

        RaftClient rc = new RaftClient("localhost",ports[1]) ;

        int ret = rc.connect() ;

        assertEquals(1, ret) ;

    }

    @Test
    void _connectLeader() throws Exception {

        RaftClient rc = new RaftClient("localhost",ports[0]) ;

        Response r = rc._connect("localhost",ports[0]) ;

        assertEquals(1, r.getStatus());

    }

    @Test
    void _connectNotLeader() throws Exception {

        RaftClient rc = new RaftClient("localhost",ports[2]) ;

        Response r = rc._connect("localhost",ports[2]) ;

        assertEquals(0, r.getStatus());
        assertEquals(2, r.getType());

    }
}
