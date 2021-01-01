package com.mj.distributed.tcp.nio;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NioListener {

    private String bindHost = "localhost" ;
    private int bindPort ;

    private Map<SocketChannel, Queue<ByteBuffer>> channelMessagesToWriteMap = new ConcurrentHashMap<>();

    private ExecutorService executorThread = Executors.newFixedThreadPool(1);

    private ServerSocketChannel serverSocketChannel;
    private Selector selector;

    private final NioListenerConsumer nioListenerConsumer;

    private boolean stop = false;

    private Logger LOG  = LoggerFactory.getLogger(NioListener.class);

    public NioListener(String bindHost, int port, NioListenerConsumer n) {

        this.bindHost = bindHost;
        this.bindPort = port;
        this.nioListenerConsumer = n;
    }

    public void start() {

        stop = false;
        executorThread.submit(this::call) ;

    }

    public void stop() throws Exception {
        stop = true ;
        LOG.info(bindHost+":"+bindPort + " is stopping") ;
        serverSocketChannel.socket().close();
        serverSocketChannel.close();
        serverSocketChannel = null;
    }

    public Void call() {

        try {

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR,true);
            serverSocketChannel.socket().setReuseAddress(true);
            serverSocketChannel.socket().bind(new InetSocketAddress("localhost", bindPort));
            selector = Selector.open();
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while(!stop) {

                try {

                    channelMessagesToWriteMap.forEach((k, v) -> {

                        try {
                            if (v.peek() != null) {
                                k.register(selector, SelectionKey.OP_WRITE);
                            }
                        } catch (Exception e) {
                            LOG.error("error", e);
                        }

                    });

                    selector.select();

                    Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                    while (keysIterator.hasNext()) {

                        SelectionKey key = keysIterator.next();
                        keysIterator.remove();


                        if (key.isAcceptable()) {

                            // LOG.info("Something to accept");
                            accept(key);
                        } else if (key.isReadable()) {
                            // LOG.info("Something to read") ;
                            read(key);
                        } else if (key.isWritable()) {
                            // LOG.info("Something to write") ;
                            write(key);
                        }

                    }

                } catch(Exception e) {
                    LOG.error("Error in listener loppop", e);
                }

            }


        } catch(Exception e) {

            LOG.error("Error starting listener thread", e);

        }

        return null ;

    }

    public void queueSendMessage(SocketChannel s, ByteBuffer b) {

        channelMessagesToWriteMap.get(s).add(b);

        selector.wakeup();
    }

    private void accept(SelectionKey key) throws IOException {

        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel sc = ssc.accept();
        sc.configureBlocking(false);

        InetSocketAddress socketAddress = (InetSocketAddress)sc.getRemoteAddress() ;

        LOG.info("accepted connection from " + socketAddress.getHostString() +":" + socketAddress.getPort());

        sc.register(this.selector, SelectionKey.OP_READ);

        channelMessagesToWriteMap.put(sc, new ConcurrentLinkedDeque<ByteBuffer>());
        nioListenerConsumer.addedConnection(sc);
    }

    private void read(SelectionKey key) throws IOException {

        SocketChannel sc = (SocketChannel) key.channel();
        ByteBuffer readBuffer = ByteBuffer.allocate(8192);

        int numread = 0;
        int totalread = 0;
        try {

            numread = sc.read(readBuffer);
            totalread = numread;
            while (numread > 0) {
                numread = sc.read(readBuffer);

                totalread = totalread + numread;


            }

            if (numread == -1) {
                // Remote entity shut the socket down cleanly. Do the
                // same from our end and cancel the channel.
                // key.channel().close();
                // key.cancel();
                throw new IOException("Read returned -1. Channel closed by client.") ;

            }
            readBuffer.rewind() ;
            nioListenerConsumer.consumeMessage(sc, totalread, readBuffer);

        } catch(IOException e) {

            sc.close() ;
            key.cancel() ;

            /* PeerData p = channelPeerMap.get(sc) ;

            LOG.info(p.getHostString() +":" +p.getPort() + " has left the cluster") ;
            channelPeerMap.remove(sc) ;
            removePeer(p.getHostString(), p.getPort()); */
            nioListenerConsumer.droppedConnection(sc);
            channelMessagesToWriteMap.remove(sc);
        }

        // readBuffer.rewind() ;
        // nioListenerConsumer.consumeMessage(sc, totalread, readBuffer);
    }

    private void write(SelectionKey key) throws IOException {

        SocketChannel sc = (SocketChannel) key.channel();

        ByteBuffer towrite = channelMessagesToWriteMap.get(sc).poll() ;

        if (towrite == null) {
            LOG.warn("Write queue is emptyy") ;
            key.interestOps(SelectionKey.OP_READ);
            return ;
        }


        int n = sc.write(towrite);
        while (n > 0 && towrite.remaining() > 0) {
            n = sc.write(towrite);
            // LOG.info("Server wrote bytes "+n) ;
        }

        key.interestOps(SelectionKey.OP_READ);

    }


}
