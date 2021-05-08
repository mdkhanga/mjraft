package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.ClusterInfo;
import com.mj.raft.client.RaftClient;
import com.mj.raft.test.client.TestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class MultithreadClientsTest {

    static PeerServer leader;
    static PeerServer server1;
    static PeerServer server2;

    ExecutorService executorService = Executors.newFixedThreadPool(11) ;

    @BeforeAll
    public static void init() throws Exception {
        System.out.println("running init") ;

        leader = new PeerServer(5101);
        leader.start() ;

        String[] seeds = new String[1];
        seeds[0] = "localhost:5101";

        server1 = new PeerServer(5102, seeds);
        server1.start();

        server2 = new PeerServer(5103, seeds);
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
    public void MultiThreadedlogReplication() throws Exception {

        List<Integer> inputs = Arrays.asList(23,33,44,91,66,101,12,172,81,312);

        CountDownLatch startGate = new CountDownLatch(1) ;

        inputs.forEach((t)->{
            TClient tc = new TClient(t, startGate);
            executorService.submit(tc);
        });

        startGate.countDown();

        System.out.println("Connecting to server2") ;
        TestClient ts = new TestClient("localhost",5102);
        ts.connect();
        System.out.println("Connected to server2") ;

        System.out.println("Connecting to server3") ;
        TestClient ts2 = new TestClient("localhost",5103);
        ts2.connect();
        System.out.println("Connected to server3") ;

        System.out.println("Connecting to server1");
        TestClient ts0 = new TestClient("localhost",5101);
        ts0.connect();
        System.out.println("Connected to server1") ;

        Thread.sleep(15000);

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
            System.out.println(server2Ints.get(0));

        }



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

            RaftClient raftClient = new RaftClient("localhost", 5101);
            raftClient.connect();

            raftClient.send(val);
            return null;
        }
    }
}