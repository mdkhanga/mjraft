package com.mj.distributed.tcp.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NioCaller {

    Logger LOG = LoggerFactory.getLogger(NioCaller.class);

    private String remoteHost;
    private int remotePort;
    private String localhost;
    private int localPort;
    private NioCallerConsumer nioCallerConsumer;

    private ExecutorService executorThread = Executors.newFixedThreadPool(1);

    private Queue<ByteBuffer> writeQueue = new ConcurrentLinkedDeque<ByteBuffer>() ;

    private Selector selector ;
    private SocketChannel clientChannel ;

    private ByteBuffer readBuf = ByteBuffer.allocate(8192)  ;

    private Boolean connected = Boolean.FALSE ;

    private Object lock = new Object();

    public NioCaller(String remotehost,
                     int remoteport,
                     String localhost,
                     int localport,
                     NioCallerConsumer n) {
        this.remoteHost = remotehost;
        this.remotePort = remoteport;
        this.localhost = localhost;
        this.localPort = localport;
        nioCallerConsumer = n;
    }

    public void start() {

        try {

            selector = Selector.open();

            clientChannel = SocketChannel.open();
            clientChannel.configureBlocking(false);

            clientChannel.connect(new InetSocketAddress(remoteHost, remotePort));

            clientChannel.register(selector, SelectionKey.OP_CONNECT);

            executorThread.submit(this::call);

            synchronized (lock) {
                while(!connected) {
                    lock.wait() ;
                }
            }

        } catch(Exception e) {

            LOG.error("Error starting NioCaller ",e) ;
        }

    }

    public void stop() throws Exception {
        synchronized (lock) {
            connected = Boolean.FALSE ;
        }
        clientChannel.close();
    }

    public Void call() {

        try {


            while (true) {

                synchronized (writeQueue) {
                    if (clientChannel.isConnected() && writeQueue.peek() != null) {
                        clientChannel.register(selector, SelectionKey.OP_WRITE);

                    }
                }
                selector.select();
                Iterator<SelectionKey> skeys = selector.selectedKeys().iterator();

                while (skeys.hasNext()) {
                    SelectionKey key = (SelectionKey) skeys.next();
                    skeys.remove();

                    if (!key.isValid()) {
                        LOG.info("key is not valid");
                        continue;
                    }

                    // System.out.println("We have a valid key") ;
                    // Check what event is available and deal with it
                    if (key.isConnectable()) {
                        // LOG.info("trying to conect") ;
                        try {
                            finishConnection(key);
                        } catch(ConnectException ce) {
                            LOG.error("Error connecting. Will wait for event",ce);
                            Thread.sleep(10000);
                            clientChannel = SocketChannel.open();
                            clientChannel.configureBlocking(false);
                            clientChannel.connect(new InetSocketAddress(remoteHost, remotePort));
                            clientChannel.register(selector, SelectionKey.OP_CONNECT);
                        }
                    } else if (key.isReadable()) {
                        // LOG.info("trying to read") ;
                        read(key);
                        // done = true ;
                    } else if (key.isWritable()) {
                        // LOG.info("trying to write") ;
                        write(key);
                    } else {
                        System.out.println("not handled key");

                    }
                }

            }

        } catch(Exception e) {
            LOG.error("Error in NioCaller",e);
        }


        return null ;
    }

    private void finishConnection(SelectionKey key) throws IOException {

        clientChannel.finishConnect() ;
        key.interestOps(SelectionKey.OP_WRITE) ;

        // peerServer.addPeer(clientChannel, new CallerPeer(new Member(remoteHost, remotePort), peerClient));
        nioCallerConsumer.addedConnection(clientChannel);
        synchronized (lock) {
            connected = Boolean.TRUE;
            lock.notify() ;
        }
        LOG.info("finished connection");
    }

    private void write(SelectionKey key) throws IOException {

        ByteBuffer b ;

        synchronized (writeQueue) {
            b = writeQueue.poll();
        }

        if (b != null) {
            // LOG.info("we got b to write") ;
        } else {

            // LOG.info("b is null could not get b to write") ;
            return ;
        }

        try {
            int n = clientChannel.write(b);
            // LOG.info("Wrote bytes " + n) ;
            int totalbyteswritten = n;
            while (n > 0 && b.remaining() > 0) {
                n = clientChannel.write(b);
                //   LOG.info("Wrote bytes " + n) ;
                totalbyteswritten = totalbyteswritten + n;

            }
            key.interestOps(SelectionKey.OP_READ);
        } catch(IOException e) {
            clientChannel.close() ;
            key.cancel();
            String remote = remoteHost +":"+remotePort ;
            String local = localhost + ":"+ localPort;
            LOG.info(local +" :remote"+ remote + " has closed the connection") ;
            // peerServer.removePeer(s);
            nioCallerConsumer.droppedConnection(clientChannel);
        }
    }

    public void read(SelectionKey key) throws IOException {

        readBuf.clear() ;

        try {

            int numread = clientChannel.read(readBuf);
            int totalread = numread;
            while (numread > 0) {

                numread = clientChannel.read(readBuf);

                if (numread <= 0) {
                    break;
                }
                totalread = totalread + numread;


            }

            if (numread < 0) {

                clientChannel.close();
                key.cancel();
            }

            readBuf.rewind();
            nioCallerConsumer.consumeMessage(clientChannel, totalread, readBuf);

        } catch (IOException e) {
            clientChannel.close() ;
            key.cancel();
            String s = remoteHost +":"+remotePort ;
            LOG.info(s + " has left the cluster") ;
            // peerServer.removePeer(s);
            nioCallerConsumer.droppedConnection(clientChannel);

        }

    }

    public void queueSendMessage(ByteBuffer b) {

        synchronized (writeQueue) {
            writeQueue.add(b);
        }

        selector.wakeup() ;

    }

}


