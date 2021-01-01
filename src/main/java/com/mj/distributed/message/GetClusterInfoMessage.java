package com.mj.distributed.message;

import com.mj.distributed.model.ClusterInfo;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class GetClusterInfoMessage implements Message {

    private static MessageType messageType = MessageType.GetClusterInfo ;

    public GetClusterInfoMessage() {

    }

    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);

        d.writeInt(messageType.value());

        byte[] bytestoWrite = b.toByteArray();
        ByteBuffer ret = ByteBuffer.allocate(bytestoWrite.length+4);

        ret.putInt(bytestoWrite.length);
        ret.put(bytestoWrite);
        ret.flip();
        return ret ;

    }

    public static GetClusterInfoMessage deserialize(ByteBuffer b) throws Exception {
        int messagesize = b.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int type = b.getInt() ;
        if (messageType.value() != type) {
            throw new RuntimeException("Message is not the expected type ClusterInfoMessage") ;
        }


        return new GetClusterInfoMessage();

    }

    public String toString() {
        return "GetClusterInfo" ;
    }
}
