package com.mj.distributed.model;

import com.mj.distributed.message.Message;
import com.mj.distributed.message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

public class Redirect  {

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
    public byte[] toBytes() throws IOException {

        byte[] hostStringBytes = redirectHost.getBytes("UTF-8") ;

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(hostStringBytes.length);
        d.write(hostStringBytes);
        d.writeInt(redirectPort);

        return b.toByteArray();

    }

    public static Redirect fromBytes(byte[] bytes) throws IOException {

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);

        int hostStringSize = din.readInt() ;
        byte[] hostStringBytes = new byte[hostStringSize] ;
        din.read(hostStringBytes,0,hostStringSize) ;
        String hostString = new String(hostStringBytes) ;
        // LOG.info("from host "+hostString) ;

        int port = din.readInt() ;
        // LOG.info("and port "+port) ;

        return new Redirect(hostString,port) ;
    }

}
