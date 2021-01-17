package com.mj.raft.client;

import com.mj.distributed.message.*;
import com.mj.distributed.tcp.nio.NioCaller;
import com.mj.distributed.tcp.nio.NioCallerConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftClient implements NioCallerConsumer {

    private String hostString;
    private int port;
    NioCaller nioCaller;
    private final AtomicInteger seq = new AtomicInteger(1);
    private Integer messageWaitingResponse ;
    private volatile Message response;
    Logger LOG = LoggerFactory.getLogger(RaftClient.class);

    public RaftClient(String host, int port) {

        this.hostString = host;
        this.port = port;
    }

    public int connect() throws Exception {

        nioCaller = new NioCaller(hostString, port, "raftclient",-1,this);
        nioCaller.start();
        RaftClientHello hello = new RaftClientHello();
        messageWaitingResponse = 0 ;
        nioCaller.queueSendMessage(hello.serialize());

        synchronized (messageWaitingResponse) {

            while(response == null ) {

                messageWaitingResponse.wait();

            }

            if (response instanceof RaftClientHelloResponse) {
                RaftClientHelloResponse r = (RaftClientHelloResponse) response ;
                response = null ;
                return 0 ;
            } else if (response instanceof ErrorResponse) {
                ErrorResponse r = (ErrorResponse) response ;
                response = null ;
                return r.getErrorCode() ;
            } else {
                throw new RuntimeException("Response to Hello not understood");
            }
        }


    }

    public void close() throws Exception {
        nioCaller.stop();
    }

    public void send(int value) throws Exception {

        byte[] val = ByteBuffer.allocate(4).putInt(value).array() ;
        RaftClientAppendEntry entry = new RaftClientAppendEntry(val);
        nioCaller.queueSendMessage(entry.serialize());

    }

    public List<byte[]> get(int start, int count) throws Exception {

        Integer id = seq.getAndIncrement();
        GetServerLog gsLog = new GetServerLog(id, 0, count, (byte)0);
        nioCaller.queueSendMessage(gsLog.serialize());
        messageWaitingResponse = id;
        synchronized (messageWaitingResponse) {

            while(response == null ) {

                messageWaitingResponse.wait();

            }

            GetServerLogResponse r = (GetServerLogResponse) response ;
            return r.getEntries();
        }
    }

    public void addedConnection(SocketChannel s) {

    }

    public void droppedConnection(SocketChannel s) {

    }

    public void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b) {

        try {

            LOG.info("Raft client Received a response message "+numBytes);

            synchronized (messageWaitingResponse) {

                // FIXME: could be a partial message or multiple messages
                int messageSize = b.getInt();
                int messageType = b.getInt() ;

                if (messageType == MessageType.RaftClientHelloResponse.value()) {
                    response = RaftClientHelloResponse.deserialize(b.rewind());
                    messageWaitingResponse.notify();
                } else if (messageType == MessageType.GetServerLogResponse.value()) {
                    response = GetServerLogResponse.deserialize(b.rewind());
                    messageWaitingResponse.notify();
                } else if (messageType == MessageType.Error.value()) {
                    response = ErrorResponse.deserialize(b.rewind());
                    messageWaitingResponse.notify();
                } else  {
                    throw new RuntimeException("RaftClient received unknown message");
                }

            }
        } catch(Exception e) {
            LOG.error("Error deserializing message",e) ;
        }


    }

    public static void main(String[] args) throws Exception {

        RaftClient client = new RaftClient("localhost",5002);
        client.connect();
        client.send(23);

        Scanner scanner = new Scanner(System.in);

        while(true) {

            System.out.print("Enter a number:") ;
            String s = scanner.nextLine();
            client.send(Integer.valueOf(s));

        }
    }

}
