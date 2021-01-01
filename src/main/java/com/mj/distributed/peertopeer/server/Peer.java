package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.Message;
import com.mj.distributed.model.Member;

import java.nio.ByteBuffer;

public interface Peer {

    void queueSendMessage(Message m) throws Exception;

    void onReceiveMessage(Message m) ;

    // ByteBuffer peekMessageQueue() ;

    // ByteBuffer getNextQueuedMessage() ;

    boolean active() ;

    Member member() ;

    void shutdown() ;

}
