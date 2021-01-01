package com.mj.raft.client;

import com.mj.distributed.message.RaftClientAppendEntry;
import com.mj.distributed.message.RaftClientHello;
import com.mj.distributed.tcp.nio.NioCaller;
import com.mj.distributed.tcp.nio.NioCallerConsumer;

import java.io.Console;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class RaftClient implements NioCallerConsumer {

    private String hostString;
    private int port;
    NioCaller nioCaller;

    public RaftClient(String host, int port) {

        this.hostString = host;
        this.port = port;
    }

    public void connect() throws Exception {

        nioCaller = new NioCaller(hostString, port, "raftclient",-1,this);
        nioCaller.start();
        RaftClientHello hello = new RaftClientHello();
        nioCaller.queueSendMessage(hello.serialize());

    }

    public void close() throws Exception {
        nioCaller.stop();
    }

    public void send(int value) throws Exception {

        byte[] val = ByteBuffer.allocate(4).putInt(value).array() ;
        RaftClientAppendEntry entry = new RaftClientAppendEntry(val);
        nioCaller.queueSendMessage(entry.serialize());

    }

    public List<Integer> get(int start, int count) {

        return new ArrayList<Integer>() ;
    }

    public void addedConnection(SocketChannel s) {

    }

    public void droppedConnection(SocketChannel s) {

    }

    public void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b) {



    }

    public static void main(String[] args) throws Exception {

        RaftClient client = new RaftClient("localhost",5001);
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
