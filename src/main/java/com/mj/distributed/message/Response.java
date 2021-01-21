package com.mj.distributed.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class Response implements Message {

    private static MessageType messageType = MessageType.Response ;
    private int status; // sucess 1 or failure 0
    private int type; // what type of data is in details
    private byte[] details;

    private static Logger LOG  = LoggerFactory.getLogger(Response.class) ;

    public Response(int status, int type, byte[] details) {

        this.status = status;
        this.type = type ;
        this.details = details;

    }

    public Response(int status, int type) {

        this.status = status;
        this.type = type ;
    }


    public int getStatus() {
        return status;
    }

    public int getType() {
        return type;
    }

    public byte[] getDetails() { return details; }


    /**
     *
     *
     *
     */
    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType.value());
        d.writeInt(status);
        d.writeInt(type);


        if (details != null) {
            d.writeInt(details.length);
            d.write(details);
        } else {
            d.writeInt(0);
        }

        byte[] errMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(errMsgArray.length+4);//

        retBuffer.putInt(errMsgArray.length);
        retBuffer.put(errMsgArray);

        retBuffer.flip() ; // make it ready for reading

        return retBuffer ;
    }

    public static Response deserialize(ByteBuffer readBuffer) {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int mType = readBuffer.getInt() ;
        if (mType != messageType.value()) {
            throw new RuntimeException("Message is not the expected type Response") ;
        }

        int status = readBuffer.getInt();
        int type = readBuffer.getInt();

        int detailsSize = readBuffer.getInt();

        if (detailsSize > 0 ) {
            byte[] details = new byte[detailsSize];
            readBuffer.get(details, 0, detailsSize);
            return new Response(status, type, details);
        } else {
            return new Response(status,  type);
        }
    }

}
