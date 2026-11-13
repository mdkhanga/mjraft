package com.mj.distributed.message.handler;

import com.mj.distributed.message.Message;
import com.mj.distributed.peertopeer.server.PeerServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.mj.distributed.tcp.Connection;

public interface MessageHandler {

    void handle(ByteBuffer readBuffer, Connection connection, PeerServer p) throws Exception;

}
