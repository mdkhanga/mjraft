package com.mj.distributed.message.handler;

import com.mj.distributed.message.Message;
import com.mj.distributed.peertopeer.server.PeerServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface MessageHandler {

    void handle(ByteBuffer readBuffer, SocketChannel socketChannel, PeerServer p) throws Exception;

}
