package com.mj.distributed.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RaftClientAppendEntry implements Message {

    private static MessageType messageType = MessageType.RaftClientAppendEntry ;
    private byte[] value;

    private static Logger LOG  = LoggerFactory.getLogger(RaftClientAppendEntry.class) ;

    public RaftClientAppendEntry(byte[] b) {

        this.value = b ;
    }

    public byte[] getValue() {
        return value ;
    }

    /**
     *
     *
     *
     */
    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType.value());

        int vSize = value.length ;
        if (vSize == 0) {
            throw new RuntimeException("value cannot be of length 0") ;
        }

        d.writeInt(vSize);
        d.write(value);

        byte[] appendEntryArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(appendEntryArray.length+4);//

        retBuffer.putInt(appendEntryArray.length);
        retBuffer.put(appendEntryArray);

        retBuffer.flip() ; // make it ready for reading
        return retBuffer ;
    }

    public static RaftClientAppendEntry deserialize(ByteBuffer readBuffer) throws Exception {

        int messagesize = readBuffer.getInt() ;
        int type = readBuffer.getInt() ;
        if (type != messageType.value()) {

            throw new RuntimeException("Message is not the expected type RaftClientAppendEntry") ;
        }

        int vSize = readBuffer.getInt() ;
        byte[] value = new byte[vSize];
        readBuffer.get(value, 0, vSize);

        return new RaftClientAppendEntry(value) ;
    }

}
