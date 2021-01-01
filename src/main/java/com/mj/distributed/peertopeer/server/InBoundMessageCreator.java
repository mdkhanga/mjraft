package com.mj.distributed.peertopeer.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InBoundMessageCreator {


    ExecutorService  inBoundMessageParser = Executors.newFixedThreadPool(1) ;

    // partial messages
    // who message did not arrive in the last read
    ConcurrentHashMap<SocketChannel,ByteBuffer> partialMessagesforChannel = new ConcurrentHashMap() ; // value could be a queue/list of bufferes

    Logger LOG  = LoggerFactory.getLogger(InBoundMessageCreator.class) ;

    PeerServer peerServer ;
    InBoundMessageHandler inBoundMessageHandler ;

    public InBoundMessageCreator(PeerServer p) {

        peerServer = p ;

        inBoundMessageHandler = new InBoundMessageHandler() ;

    }

    public void submit(SocketChannel s, ByteBuffer b, int bytesinbuffer) {

        // make a copy of the ByteBuffer
        ByteBuffer newBuffer = ByteBuffer.allocate(bytesinbuffer);
        System.arraycopy(b.array(),0,newBuffer.array(),0,bytesinbuffer);

        inBoundMessageParser.submit(new InBoundMessageReader(peerServer, s,newBuffer,bytesinbuffer)) ;
    }

    public class InBoundMessageReader implements Callable {

        SocketChannel socketChannel ;
        ByteBuffer readBuffer ;
        int numbytes ;
        PeerServer peerServer;
        // Callable handler ;

        public InBoundMessageReader(PeerServer p, SocketChannel s, ByteBuffer b,int numbyt) {

            peerServer = p;
            socketChannel = s ;
            readBuffer = b ;
            numbytes = numbyt ;
            // handler = c ;

        }

        public Void call() {

            boolean prevreadpartial = false ;
            int messagesize ;

            // LOG.info("numBytes = " + numbytes);

            if (!prevreadpartial) {

                /* messagesize = readBuffer.getInt();

               LOG.info("messagesize = "+ messagesize) ;
               LOG.info("numBytes = " + numbytes) ;

                int messageBytesToRead = numbytes -4 ; */

                // LOG.info("messageBytesRead = " + messageBytesRead) ;

                int srcPos = 0;
                while (numbytes > 0) {

                    // LOG.info("numBytes = " + numbytes) ;
                    readBuffer.position(srcPos) ;
                    // LOG.info("readBuffer pos" + readBuffer.position());
                    messagesize = readBuffer.getInt();

                    // LOG.info("messagesize = "+ messagesize) ;


                    numbytes = numbytes -4 ;

                    // FIXME: additional copy
                    ByteBuffer newBuffer = ByteBuffer.allocate(messagesize+4);
                    // System.arraycopy(readBuffer.array(),srcPos ,newBuffer.array(),0,messagesize+4);
                    // LOG.info("src Pos = " + srcPos) ;
                    newBuffer.put(readBuffer.array(), srcPos, messagesize+4 );

                    inBoundMessageHandler.submit(new ServerMessageHandlerCallable(peerServer, socketChannel,
                            newBuffer.rewind()));

                    numbytes = numbytes - messagesize ;
                    srcPos = srcPos + messagesize + 4;
                }


                 /* if (messagesize == messageBytesToRead) {

                    // We have the full message

                     // WARNING: commenting the line below might break cde
                    // readBuffer.rewind();
                    inBoundMessageHandler.submit(handler);
                 } else if (messageBytesToRead < messagesize) {
                    LOG.info("Did not receive full message received:" + messageBytesToRead + " expected: "+messagesize);
                 } else {

                    LOG.info("more than 1 message " + numbytes);
                 } */
            } else {

                // start reading and adding to partial message from last read
                LOG.info("Seems like a partial message") ;

            }



            return null ;
        }

    }


}
