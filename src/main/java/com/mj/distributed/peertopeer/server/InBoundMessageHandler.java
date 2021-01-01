package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.AckMessage;
import com.mj.distributed.message.HelloMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InBoundMessageHandler {


    ExecutorService messageHandlers = Executors.newCachedThreadPool() ;

    Logger LOG  = LoggerFactory.getLogger(InBoundMessageHandler.class) ;

    public InBoundMessageHandler() {


    }

    public void submit(Callable c) {

        messageHandlers.submit(c) ;

    }



}
