package com.mj.distributed.model;

import com.mj.distributed.message.Message;
import com.mj.distributed.message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

public class Error  {

    private MessageType messageType = MessageType.Error ;
    private int errorCode;
    private String errorMessage;
    private byte[] info; // additional info or instructions

    private static Logger LOG  = LoggerFactory.getLogger(Error.class) ;

    public Error(int code, String msg, byte[] info) {

        this.errorCode = code;
        this.errorMessage = msg;
        this.info = info;

    }

    public Error(int code, String msg) {

        this.errorCode = code;
        this.errorMessage = msg;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public byte[] getInfo() { return info; }


    /**
     *
     *
     *
     */
    public byte[] toBytes() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(errorCode);

        byte[] errMsgBytes = errorMessage.getBytes("UTF-8");
        d.writeInt(errMsgBytes.length);
        d.write(errMsgBytes);

        if (info != null) {
            d.writeInt(info.length);
            d.write(info);
        } else {
            d.writeInt(0);
        }

        byte[] errMsgArray = b.toByteArray();

        return errMsgArray;
    }

    public static Error fromBytes(byte[] bytes) throws IOException {

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);


        int code = din.readInt();
        int messageSize = din.readInt();
        byte[] messageBytes = new byte[messageSize];
        din.read(messageBytes, 0, messageSize);
        String message = new String(messageBytes);

        int detailsSize = din.readInt();

        if (detailsSize > 0 ) {
            byte[] details = new byte[detailsSize];
            din.read(details, 0, detailsSize);
            return new Error(code, message, details);
        } else {
            return new Error(code,  message);
        }
    }

}
