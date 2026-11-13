package com.mj.raft.test.client;

import com.mj.distributed.message.*;
import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.tcp.BlockingConnection;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads state from any server, leader or not. Used by tests.
 */
public class TestClient implements AutoCloseable {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(10);

    private final String hostString;
    private final int port;
    private BlockingConnection connection;
    private final AtomicInteger seq = new AtomicInteger(1);

    public TestClient(String host, int port) {

        this.hostString = host;
        this.port = port;
    }

    public void connect() throws Exception {

        connection = BlockingConnection.open(hostString, port, CONNECT_TIMEOUT, RESPONSE_TIMEOUT);
        connection.call(new TestClientHello(), MessageType.TestClientHelloResponse);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    public List<byte[]> get(int start, int count) throws Exception {

        GetServerLog gsLog = new GetServerLog(seq.getAndIncrement(), start, count, (byte) 0);
        ByteBuffer reply = connection.call(gsLog, MessageType.GetServerLogResponse);
        return GetServerLogResponse.deserialize(reply).getEntries();
    }

    public ClusterInfo getClusterInfo() throws Exception {

        ByteBuffer reply = connection.call(new GetClusterInfoMessage(), MessageType.ClusterInfo);
        return ClusterInfoMessage.deserialize(reply).getClusterInfo();
    }

}
