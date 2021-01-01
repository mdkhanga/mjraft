package com.mj.distributed.message;

import java.awt.*;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by manoj on 6/18/17.
 */
public class PingMessage implements Message {

    private static MessageType messageType = MessageType.Ping ;
    private int serverId ; // server sending ping
    private int seqId ;

    public PingMessage() {

    }

    public PingMessage(int id, int s) {
        serverId = id ;
        seqId = s ;
    }

    public ByteBuffer serialize() throws IOException {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);

        d.writeInt(messageType.value());
        d.writeInt(serverId);
        d.writeInt(seqId);

        byte[] pingMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(pingMsgArray.length+4);//

        retBuffer.putInt(pingMsgArray.length);
        retBuffer.put(pingMsgArray);

        retBuffer.flip() ; // make it ready for reading

        return retBuffer ;
     }

    public static PingMessage deserialize(ByteBuffer readBuffer) throws IOException {

        int size = readBuffer.getInt() ;


        int mType = readBuffer.getInt() ;
        if (mType != messageType.value()) {
            throw new RuntimeException("Not a ping message "+ mType) ;
        }

        int server = readBuffer.getInt() ;
        int seq = readBuffer.getInt() ;

        PingMessage p = new PingMessage(server, seq) ;

        return p ;

    }

    public int getServerId() {
        return serverId ;
    }

    public void setServerId(int id) {
        serverId = id ;
    }

    public int getSeqId() {
        return seqId ;
    }

    public String print() {

        return "Ping message from server :" + serverId ;
    }

    public int size() {

        // int s = 0 ;
        // s=+ 4 ; // size
        // s=+4 ; // message type
        // s=+4 ; // from server
        // s=+4 ; // seq

        return 16 ;
    }
}
