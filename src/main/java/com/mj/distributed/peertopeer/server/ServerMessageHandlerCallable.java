package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.*;
import com.mj.distributed.message.handler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ServerMessageHandlerCallable implements Callable {

    SocketChannel socketChannel ;
    ByteBuffer readBuffer ;
    PeerServer peerServer;

    Logger LOG  = LoggerFactory.getLogger(ServerMessageHandlerCallable.class) ;

    static Map<MessageType, MessageHandler> handlerMap = new HashMap<>() ;
    static {
        handlerMap.put(MessageType.Hello, new HelloHandler());
        handlerMap.put(MessageType.Response, new ResponseHandler());
        handlerMap.put(MessageType.TestClientHello, new TestHelloHandler());
        handlerMap.put(MessageType.AppendEntriesResponse, new AppendEntriesHelloHandler());
        handlerMap.put(MessageType.AppendEntries, new AppendEntriesHandler());
        handlerMap.put(MessageType.RequestVote, new RequestVoteHandler());
        handlerMap.put(MessageType.RequestVoteResponse, new RequestVoteResponseHandler());
        handlerMap.put(MessageType.RaftClientHello, new RaftClientHelloHandler());
        handlerMap.put(MessageType.ClusterInfo, new ClusterInfoHandler());
        handlerMap.put(MessageType.RaftClientAppendEntry, new RaftClientAppendEntryHandler());
        handlerMap.put(MessageType.GetServerLog, new GetServerLogHandler());
        handlerMap.put(MessageType.GetClusterInfo, new GetClusterInfoHandler());

    }

    public ServerMessageHandlerCallable(PeerServer p, SocketChannel s , ByteBuffer b) {

        peerServer = p ;
        socketChannel = s ;
        readBuffer = b ;

    }

    public Void call() {

        // WARNING : 11142020
        // MIGHT BREAK CODE
        // commented read because rewind in InBoundMessage Creator was commented
        int messagesize = readBuffer.getInt() ;
        int messageType = readBuffer.getInt() ;


        try {

            MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
            if (m != null) {
                m.handle(readBuffer, socketChannel, peerServer);
            } else {
                LOG.info("Received message of unknown type " + messageType);
            }

            /* if (messageType == MessageType.Hello.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);
            } else if (messageType == MessageType.Response.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);

           } else if(messageType == MessageType.TestClientHello.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);

            } else if (messageType == MessageType.Ack.value()) {

                AckMessage message = AckMessage.deserialize(readBuffer.rewind());

            } else if (messageType == MessageType.AppendEntriesResponse.value()) {
                 MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);

            } else if (messageType == MessageType.RequestVote.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);

            } else if (messageType == MessageType.AppendEntries.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);
            }  else if (messageType == MessageType.ClusterInfo.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);

            } else if (messageType == MessageType.RequestVoteResponse.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);
            } else if (messageType == MessageType.RaftClientHello.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);


            } else if (messageType == MessageType.RaftClientAppendEntry.value()) {

                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);


            } else if (messageType == MessageType.GetServerLog.value()) {
                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);


            } else if (messageType == MessageType.GetClusterInfo.value()) {
                MessageHandler m = handlerMap.get(MessageType.valueOf(messageType));
                m.handle(readBuffer, socketChannel, peerServer);

            }
            else {
                LOG.info("Received message of unknown type " + messageType);
            } */

        } catch(Exception e) {
            LOG.error("Error deserializing message ",e);
        }

        return null ;
    }
}
