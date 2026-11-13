package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.MessageType;
import com.mj.distributed.message.handler.*;
import com.mj.distributed.tcp.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;

/**
 * Routes an inbound frame to the handler for its message type.
 * Runs on the connection's reader thread, so messages from one connection are handled in order.
 */
final class MessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(MessageDispatcher.class);

    private static final Map<MessageType, MessageHandler> handlerMap = new EnumMap<>(MessageType.class);
    static {
        handlerMap.put(MessageType.Hello, new HelloHandler());
        handlerMap.put(MessageType.Response, new ResponseHandler());
        handlerMap.put(MessageType.TestClientHello, new TestHelloHandler());
        handlerMap.put(MessageType.AppendEntriesResponse, new AppendEntriesResponseHandler());
        handlerMap.put(MessageType.AppendEntries, new AppendEntriesHandler());
        handlerMap.put(MessageType.RequestVote, new RequestVoteHandler());
        handlerMap.put(MessageType.RequestVoteResponse, new RequestVoteResponseHandler());
        handlerMap.put(MessageType.RaftClientHello, new RaftClientHelloHandler());
        handlerMap.put(MessageType.ClusterInfo, new ClusterInfoHandler());
        handlerMap.put(MessageType.RaftClientAppendEntry, new RaftClientAppendEntryHandler());
        handlerMap.put(MessageType.GetServerLog, new GetServerLogHandler());
        handlerMap.put(MessageType.GetClusterInfo, new GetClusterInfoHandler());
    }

    private MessageDispatcher() {
    }

    /**
     * @param frame [size][type][payload], positioned at 0
     */
    static void dispatch(ByteBuffer frame, Connection connection, PeerServer peerServer) {
        int messageType = frame.getInt(4);
        MessageType type = MessageType.valueOf(messageType);
        MessageHandler handler = type == null ? null : handlerMap.get(type);
        if (handler == null) {
            LOG.info("Received message of unknown type " + messageType);
            return;
        }
        try {
            handler.handle(frame, connection, peerServer);
        } catch (Exception e) {
            LOG.error("Error handling " + type + " message from " + connection, e);
        }
    }
}
