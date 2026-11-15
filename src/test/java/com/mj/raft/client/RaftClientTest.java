package com.mj.raft.client;

import com.mj.distributed.message.Response;
import com.mj.distributed.peertopeer.server.PeerServer;
import com.mj.raft.testutil.Ports;
import com.mj.raft.testutil.Servers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("cluster")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class RaftClientTest {

    static int[] ports;
    static PeerServer[] servers;
    static PeerServer leader;
    static int followerPort;

    @BeforeAll
    public static void init() throws Exception {
        ports = Ports.free(3);
        servers = Servers.startCluster(ports);
        leader = Servers.awaitLeader(Duration.ofSeconds(30), servers);
        followerPort = Arrays.stream(ports).filter(p -> p != leader.getBindPort()).findFirst().orElseThrow();
    }

    @AfterAll
    public static void destroy() throws Exception {
        Servers.stopAll(servers);
    }

    @Test
    public void ClientRedirect() throws Exception {

        RaftClient rc = new RaftClient("localhost", followerPort) ;

        int ret = rc.connect() ;

        assertEquals(1, ret) ;

    }

    @Test
    void _connectLeader() throws Exception {

        RaftClient rc = new RaftClient("localhost", leader.getBindPort()) ;

        Response r = rc._connect("localhost", leader.getBindPort()) ;

        assertEquals(1, r.getStatus());

    }

    @Test
    void _connectNotLeader() throws Exception {

        RaftClient rc = new RaftClient("localhost", followerPort) ;

        Response r = rc._connect("localhost", followerPort) ;

        assertEquals(0, r.getStatus());
        assertEquals(2, r.getType());

    }
}
