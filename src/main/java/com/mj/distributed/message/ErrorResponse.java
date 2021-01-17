package com.mj.distributed.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class ErrorResponse implements Message {

    private MessageType messageType = MessageType.Error ;
    private int errorCode;
    private String errorMessage;
    private byte[] errorDetails;

    private static Logger LOG  = LoggerFactory.getLogger(ErrorResponse.class) ;

    public ErrorResponse(int code, String msg, byte[] details) {

        this.errorCode = code;
        this.errorMessage = msg;
        this.errorDetails = details;

    }

    public ErrorResponse(int code, String msg) {

        this.errorCode = code;
        this.errorMessage = msg;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public byte[] getErrorDetails() { return errorDetails; }


    /**
     *
     *
     *
     */
    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType.value());
        d.writeInt(errorCode);

        byte[] errMsgBytes = errorMessage.getBytes("UTF-8");
        d.writeInt(errMsgBytes.length);
        d.write(errMsgBytes);

        if (errorDetails != null) {
            d.writeInt(errorDetails.length);
            d.write(errorDetails);
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

    public static ErrorResponse deserialize(ByteBuffer readBuffer) {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int messageType = readBuffer.getInt() ;
        if (messageType != MessageType.Error.value()) {
            throw new RuntimeException("Message is not the expected type ErrorResponse") ;
        }

        int code = readBuffer.getInt();
        int messageSize = readBuffer.getInt();
        byte[] messageBytes = new byte[messageSize];
        readBuffer.get(messageBytes, 0, messageSize);
        String message = new String(messageBytes);

        int detailsSize = readBuffer.getInt();

        if (detailsSize > 0 ) {
            byte[] details = new byte[detailsSize];
            readBuffer.get(details, 0, detailsSize);
            return new ErrorResponse(code, message, details);
        } else {
            return new ErrorResponse(code,  message);
        }
    }

}
