package com.mj.distributed.peertopeer.server;

import com.mj.distributed.tcp.nio.NioListener;
import com.mj.distributed.message.Message;
import com.mj.distributed.model.Member;

import java.nio.channels.SocketChannel;

public class ListenerPeer implements Peer {

    private final Member member ; // member we are are connected to
    private final SocketChannel socketChannel ;
    private final boolean active = true ;
    // private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedDeque<>() ;
    private NioListener nioListener;

    public ListenerPeer(NioListener listener, Member m, SocketChannel sc) {
        member = m ;
        socketChannel = sc ;
        nioListener = listener;
    }

    public void queueSendMessage(Message m) throws Exception {

        // writeQueue.add(m.serialize()) ;
        nioListener.queueSendMessage(socketChannel, m.serialize());
    }

    public void onReceiveMessage(Message m) {

    }

    public boolean active() {

        return active;
    }

    public Member member() {

        return member;
    }

    public void shutdown() {


    }

    public SocketChannel socketChannel() {
        return socketChannel ;
    }

   /* public ByteBuffer peekMessageQueue() {
        return writeQueue.peek() ;
    }

    public ByteBuffer getNextQueuedMessage() {
        return writeQueue.poll() ;
    }
    */
}
