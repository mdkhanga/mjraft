package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.ClusterInfo;
import com.mj.raft.client.RaftClient;
import com.mj.raft.test.client.TestClient;
import com.mj.raft.testutil.Await;
import com.mj.raft.testutil.Ports;
import com.mj.raft.testutil.Servers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Tag("cluster")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class PeerServerTest {

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
    public void logReplication() throws Exception {


        List<Integer> inputs = Arrays.asList(23,33,44,91,66);

        RaftClient raftClient = new RaftClient("localhost", ports[0]);
        raftClient.connect();

        for (int i = 0 ; i <=4 ; i++) {
            raftClient.send(inputs.get(i));
        }

        Await.until(Duration.ofSeconds(20), "all servers have 5 log entries",
                () -> Servers.allHaveLogSize(5, leader, server1, server2));

        TestClient ts = new TestClient("localhost",ports[1]);
        ts.connect();
        List<byte[]> server2Values = ts.get(0,5);
        List<Integer> server2Ints = convertToIntList(server2Values) ;

        TestClient ts2 = new TestClient("localhost",ports[2]);
        ts2.connect();
        List<byte[]> server3Values
                = ts2.get(0,5);
        List<Integer> server3Ints = convertToIntList(server3Values) ;

        // connects to a follower and is redirected to the leader
        RaftClient ts0 = new RaftClient("localhost",ports[1]);
        ts0.connect();
        List<byte[]> server0Values = ts0.get(0,5);
        List<Integer> server0Ints = convertToIntList(server0Values) ;

        for (int i = 0 ; i <= 4; i++ ) {
            assertEquals(server3Ints.get(i), server2Ints.get(i) );
            assertEquals(server0Ints.get(i), server2Ints.get(i) );
        }

    }

    @Test
    public void leaderElectionOnStart() throws Exception {


        TestClient ts = new TestClient("localhost",ports[1]);
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo() ;
        assertEquals(ports[0], cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());


        TestClient ts2 = new TestClient("localhost",ports[2]);
        ts2.connect();
        cs1 = ts2.getClusterInfo() ;
        assertEquals(ports[0], cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());


        TestClient ts0 = new TestClient("localhost",ports[0]);
        ts0.connect();
        cs1 = ts0.getClusterInfo() ;
        assertEquals(ports[0], cs1.getLeader().getPort());
        assertEquals("localhost", cs1.getLeader().getHostString());

    }

    private List<Integer> convertToIntList(List<byte[]> bytes) {

        List<Integer> ret = new ArrayList<>();

        bytes.forEach(e->{
            ret.add(ByteBuffer.wrap(e).getInt());
        });

        return ret ;


    }
}
