package com.mj.distributed.message;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RaftClientHello implements Message  {

    private MessageType messageType = MessageType.RaftClientHello;

    public RaftClientHello( ) {

    }

    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);

        d.writeInt(messageType.value());

        byte[] raftMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(raftMsgArray.length+4);//

        retBuffer.putInt(raftMsgArray.length);
        retBuffer.put(raftMsgArray);
        retBuffer.flip() ; // make it ready for reading
        return retBuffer ;
    }

    public static RaftClientHello deserialize(ByteBuffer readBuffer) {

        int messagesize = readBuffer.getInt() ;

        int messageType = readBuffer.getInt() ;
        if (messageType != MessageType.RaftClientHello.value()) {

            throw new RuntimeException("Message is not the expected type RaftClientHello") ;
        }

        return new RaftClientHello() ;
    }

}
