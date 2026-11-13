package com.mj.raft.client;

import com.mj.distributed.message.*;
import com.mj.distributed.model.Redirect;
import com.mj.distributed.tcp.BlockingConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftClient implements AutoCloseable {

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    static final Duration DEFAULT_RESPONSE_TIMEOUT = Duration.ofSeconds(10);
    static final int MAX_REDIRECTS = 5;

    private final String hostString;
    private final int port;
    private final Duration responseTimeout;
    private BlockingConnection connection;
    private final AtomicInteger seq = new AtomicInteger(1);

    public RaftClient(String host, int port) {
        this(host, port, DEFAULT_RESPONSE_TIMEOUT);
    }

    public RaftClient(String host, int port, Duration responseTimeout) {
        this.hostString = host;
        this.port = port;
        this.responseTimeout = responseTimeout;
    }

    /**
     * Connects, following redirects to the leader.
     *
     * @return the status of the server's Response: 1 when connected to the leader,
     *         0 when an election is in progress
     */
    public int connect() throws Exception {

        String hostToTry = hostString;
        int portToTry = port;

        for (int redirects = 0; redirects <= MAX_REDIRECTS; redirects++) {

            Response r = _connect(hostToTry, portToTry);

            if (r.getStatus() == 0 && r.getType() == 2) {
                // redirect
                Redirect rd = Redirect.fromBytes(r.getDetails());
                connection.close();
                hostToTry = rd.getHost();
                portToTry = rd.getHostPort();
            } else {
                return r.getStatus();
            }
        }

        throw new IOException("Gave up after " + MAX_REDIRECTS + " redirects, last to " + hostToTry + ":" + portToTry);
    }

    Response _connect(String host, int port) throws Exception {

        connection = BlockingConnection.open(host, port, CONNECT_TIMEOUT, responseTimeout);
        ByteBuffer reply = connection.call(new RaftClientHello(), MessageType.Response);
        return Response.deserialize(reply);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    public void send(int value) throws Exception {

        byte[] val = ByteBuffer.allocate(4).putInt(value).array();
        send(val);
    }

    public void send(byte[] val) throws Exception {

        connection.send(new RaftClientAppendEntry(val));
    }

    public List<byte[]> get(int start, int count) throws Exception {

        GetServerLog gsLog = new GetServerLog(seq.getAndIncrement(), start, count, (byte) 0);
        ByteBuffer reply = connection.call(gsLog, MessageType.GetServerLogResponse);
        return GetServerLogResponse.deserialize(reply).getEntries();
    }

    public static void main(String[] args) throws Exception {

        RaftClient client = new RaftClient(args[0], Integer.parseInt(args[1]));
        client.connect();

        Scanner scanner = new Scanner(System.in);

        while (true) {

            System.out.print("Enter a number:");
            String s = scanner.nextLine();
            client.send(s.getBytes());

        }
    }

}
