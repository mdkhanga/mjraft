package com.mj.distributed.tcp.nio;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface NioListenerConsumer {

    void addedConnection(SocketChannel s);

    void droppedConnection(SocketChannel s);

    void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b);
}
