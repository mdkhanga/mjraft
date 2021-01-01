package com.mj.distributed.message;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class TestClientHelloResponse implements Message  {

    private MessageType messageType = MessageType.TestClientHelloResponse;

    public TestClientHelloResponse( ) {

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

    public static TestClientHelloResponse deserialize(ByteBuffer readBuffer) {

        int messagesize = readBuffer.getInt() ;

        int messageType = readBuffer.getInt() ;
        if (messageType != MessageType.TestClientHelloResponse.value()) {

            throw new RuntimeException("Message is not the expected type TestClientHelloResponse") ;
        }

        return new TestClientHelloResponse() ;
    }

}
