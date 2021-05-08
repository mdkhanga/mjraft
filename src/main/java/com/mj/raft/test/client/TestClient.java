package com.mj.raft.test.client;

import com.mj.distributed.message.*;
import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.peertopeer.server.PeerClient;
import com.mj.distributed.tcp.nio.NioCaller;
import com.mj.distributed.tcp.nio.NioCallerConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// Not thread safe
public class TestClient implements NioCallerConsumer {

    private final String hostString;
    private final int port;
    private NioCaller nioCaller;
    private final AtomicInteger seq = new AtomicInteger(1);
    // private Map<Integer, Message> responseSet = new ConcurrentHashMap<>();
    private Integer messageWaitingResponse ;
    private volatile Message response;
    Logger LOG = LoggerFactory.getLogger(TestClient.class);

    public TestClient(String host, int port) {

        this.hostString = host;
        this.port = port;
    }

    public void connect() throws Exception {

        nioCaller = new NioCaller(hostString, port, "testclient", -1,this);
        nioCaller.start();
        TestClientHello hello = new TestClientHello();
        messageWaitingResponse = 0 ;
        response = null ;
        nioCaller.queueSendMessage(hello.serialize());

        synchronized (messageWaitingResponse) {

                while(response == null ) {

                    System.out.println("TestClient connect waiting got response") ;
                    messageWaitingResponse.wait();
                    System.out.println("TestClient connect woken by notify") ;

                }

                System.out.println("TestClient connect got response") ;

                TestClientHelloResponse r = (TestClientHelloResponse) response ;
                response = null ;
        }




    }

    public void close() throws Exception {
        nioCaller.stop();
    }

    /*
    public void send(int value) throws Exception {

        byte[] val = ByteBuffer.allocate(4).putInt(value).array() ;
        RaftClientAppendEntry entry = new RaftClientAppendEntry(val);
        nioCaller.queueSendMessage(entry.serialize());

    } */

    public List<byte[]> get(int start, int count) throws Exception {

        Integer id = seq.getAndIncrement();
        GetServerLog gsLog = new GetServerLog(id, 0, count, (byte)0);
        response =null ;
        nioCaller.queueSendMessage(gsLog.serialize());
        messageWaitingResponse = id;

        synchronized (messageWaitingResponse) {

            while(response == null ) {

                messageWaitingResponse.wait();

            }

            GetServerLogResponse r = (GetServerLogResponse) response ;
            // response =null ;
            return r.getEntries();
        }

    }

    public ClusterInfo getClusterInfo() throws Exception {

        Integer id = seq.getAndIncrement();
        GetClusterInfoMessage getClusterInfo = new GetClusterInfoMessage();
        response = null ;
        nioCaller.queueSendMessage(getClusterInfo.serialize());
        LOG.info("Testclient sent getClusterInfo wating for response") ;
        messageWaitingResponse = id;

        synchronized (messageWaitingResponse) {

            while(response == null ) {

                messageWaitingResponse.wait();

            }

            LOG.info("Testclient getClusterInfo got a response") ;
            ClusterInfoMessage r = (ClusterInfoMessage) response ;
            ClusterInfo ret = r.getClusterInfo() ;
            response = null ;
            return ret;
        }

    }

    public void addedConnection(SocketChannel s) {

    }

    public void droppedConnection(SocketChannel s) {

    }

    public void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b)  {

        try {

            LOG.info("Test client Received a response message "+numBytes);

            synchronized (messageWaitingResponse) {

                // FIXME: could be a partial message or multiple messages
                int messageSize = b.getInt();
                int messageType = b.getInt() ;

                // LOG.info("Test client Received a response message type "+messageType);

                if (messageType == MessageType.TestClientHelloResponse.value()) {
                    response = TestClientHelloResponse.deserialize(b.rewind());
                    messageWaitingResponse.notifyAll();
                    // System.out.println("Notifying test client connect done") ;
                } else if (messageType == MessageType.GetServerLogResponse.value()) {
                    response = GetServerLogResponse.deserialize(b.rewind());
                    messageWaitingResponse.notify();
                } else if (messageType == MessageType.ClusterInfo.value()) {
                    response = ClusterInfoMessage.deserialize(b.rewind());
                    messageWaitingResponse.notify();
                } else {
                    LOG.info("Unknown message type ..." + messageType) ;
                }
                // System.out.println("Should be releasing the lock here") ;
            }
        } catch(Exception e) {
            LOG.error("Error deserializing message",e) ;
        }
    }

    public static void main(String[] args) throws Exception {

        TestClient client = new TestClient("localhost",5001);
        client.connect();
        // client.send(23);

        Scanner scanner = new Scanner(System.in);

        while(true) {

            System.out.print("Enter a number:") ;
            String s = scanner.nextLine();
            // client.send(Integer.valueOf(s));

        }



    }

}
