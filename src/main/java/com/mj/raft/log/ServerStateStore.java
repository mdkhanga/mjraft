package com.mj.raft.log;

import com.mj.distributed.model.ServerState;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ServerStateStore {

    RandomAccessFile serverstatestore;
    FileChannel serverstateChannel;

    public ServerStateStore() {

    }

    private void init() throws IOException {

        serverstatestore = new RandomAccessFile("serverstatestore.dat","rw") ;
        serverstateChannel = serverstatestore.getChannel();

    }

    public void save(ServerState s) throws IOException {
        serverstateChannel.position(0);
        ByteBuffer b = s.serialize();

        while(b.hasRemaining()) {
            int bytes = serverstateChannel.write(b) ;
        }

    }

    public ServerState load() throws IOException {
        serverstateChannel.position(0);

        ByteBuffer readBuffer = ByteBuffer.allocate(256);

        int numread = 0;
        int totalread = 0;

        numread = serverstateChannel.read(readBuffer);
        totalread = numread;
        while (numread > 0) {
            numread = serverstateChannel.read(readBuffer);
                totalread = totalread + numread;
            }

            readBuffer.rewind();

        return ServerState.deserialize(readBuffer);

    }

}
