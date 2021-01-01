package com.mj.distributed.message;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by manoj on 6/18/17.
 */
public class AckMessage implements Message {

    private static MessageType messageType = MessageType.Ack ;
    private int seqOfMessageAcked ;

    public AckMessage() {

    }

    public AckMessage(int id) {
        seqOfMessageAcked= id ;
    }

    public int getSeqOfMessageAcked() {
        return seqOfMessageAcked;
    }

    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType.value());
        d.writeInt(seqOfMessageAcked) ;

        byte[] ackMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(ackMsgArray.length+4);//

        retBuffer.putInt(ackMsgArray.length);
        retBuffer.put(ackMsgArray);

        retBuffer.flip() ; // make it ready for reading

        return retBuffer ;
    }

    public static AckMessage deserialize(ByteBuffer b) {

        int size = b.getInt() ;

        int type = b.getInt() ;

        if (type != messageType.value()) {
            throw new RuntimeException("Not a ping message "+ type) ;
        }

        int seq = b.getInt() ;

        AckMessage r = new AckMessage(seq) ;

        return r ;
    }

}
