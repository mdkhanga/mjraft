package com.mj.distributed.peertopeer.server;

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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Tag("cluster")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class MultithreadClientsTest {

    static int[] ports;
    static PeerServer leader;
    static PeerServer server1;
    static PeerServer server2;

    ExecutorService executorService = Executors.newFixedThreadPool(11) ;

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
    public void MultiThreadedlogReplication() throws Exception {

        List<Integer> inputs = Arrays.asList(23,33,44,91,66,101,12,172,81,312);

        CountDownLatch startGate = new CountDownLatch(1) ;

        List<Future<Void>> sends = new ArrayList<>();
        inputs.forEach((t)->{
            TClient tc = new TClient(t, startGate);
            sends.add(executorService.submit(tc));
        });

        startGate.countDown();

        TestClient ts = new TestClient("localhost",ports[1]);
        ts.connect();

        TestClient ts2 = new TestClient("localhost",ports[2]);
        ts2.connect();

        TestClient ts0 = new TestClient("localhost",ports[0]);
        ts0.connect();

        // surface any client-side failure
        for (Future<Void> f : sends) {
            f.get(30, TimeUnit.SECONDS);
        }

        Await.until(Duration.ofSeconds(30), "all servers have 10 log entries",
                () -> Servers.allHaveLogSize(inputs.size(), leader, server1, server2));

        for (int i = 0 ; i < 10 ; i++) {

            List<byte[]> server0Values = ts0.get(i,1);
            List<Integer> server0Ints = convertToIntList(server0Values) ;

            List<byte[]> server2Values = ts.get(i,1);
            List<Integer> server2Ints = convertToIntList(server2Values) ;

            List<byte[]> server3Values
                    = ts2.get(i,1);
            List<Integer> server3Ints = convertToIntList(server3Values) ;

            assertEquals(server3Ints.get(0), server2Ints.get(0) );
            assertEquals(server0Ints.get(0), server2Ints.get(0) );

        }

        executorService.shutdown();

    }


    private List<Integer> convertToIntList(List<byte[]> bytes) {

        List<Integer> ret = new ArrayList<>();

        bytes.forEach(e->{
            ret.add(ByteBuffer.wrap(e).getInt());
        });

        return ret ;


    }

    class TClient implements Callable<Void> {

        int val ;
        CountDownLatch startGate;


        TClient(int v, CountDownLatch t) {
            val = v ;
            startGate = t;
        }

        @Override
        public Void call() throws Exception {

            startGate.await() ;

            RaftClient raftClient = new RaftClient("localhost", ports[0]);
            raftClient.connect();

            raftClient.send(val);
            return null;
        }
    }
}
