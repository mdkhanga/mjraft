package com.mj.distributed.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class Redirect implements Message {

    private MessageType messageType = MessageType.Hello ;
    private String redirectHost ;
    private int redirectPort ;

    private static Logger LOG  = LoggerFactory.getLogger(Redirect.class) ;

    public Redirect(String host, int port) {

        this.redirectHost = host ;
        this.redirectPort = port ;

    }

    public String getHost() {
        return redirectHost ;
    }

    public int getHostPort() {
        return redirectPort ;
    }


    /**
     *
     *
     *
     */
    public ByteBuffer serialize() throws Exception {

        byte[] hostStringBytes = redirectHost.getBytes("UTF-8") ;

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType.value());
        d.writeInt(hostStringBytes.length);
        d.write(hostStringBytes);
        d.writeInt(redirectPort);

        byte[] helloMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(helloMsgArray.length+4);//

        int l = helloMsgArray.length+4 ;

        retBuffer.putInt(helloMsgArray.length);
        retBuffer.put(helloMsgArray);

        retBuffer.flip() ; // make it ready for reading

        return retBuffer ;
    }

    public static Redirect deserialize(ByteBuffer readBuffer) {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int messageType = readBuffer.getInt() ;
        if (messageType != 1) {

            throw new RuntimeException("Message is not the expected type RedirecMessage") ;
        }

        int hostStringSize = readBuffer.getInt() ;
        byte[] hostStringBytes = new byte[hostStringSize] ;
        readBuffer.get(hostStringBytes,0,hostStringSize) ;
        String hostString = new String(hostStringBytes) ;
        // LOG.info("from host "+hostString) ;

        int port = readBuffer.getInt() ;
        // LOG.info("and port "+port) ;

        return new Redirect(hostString,port) ;
    }

}
