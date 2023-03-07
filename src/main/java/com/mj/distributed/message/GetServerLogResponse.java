package com.mj.distributed.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GetServerLogResponse implements Message {

    private static MessageType messageType = MessageType.GetServerLogResponse ;
    private final int responseForSeqId;
    private final ArrayList<byte[]> entries ;

    private static Logger LOG  = LoggerFactory.getLogger(GetServerLogResponse.class) ;


    public GetServerLogResponse(int id, List<byte[]> e) {

        responseForSeqId = id;
        entries = (ArrayList)e;
    }

    public List<byte[]> getEntries() {
        return entries;
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

        d.writeInt(responseForSeqId);
        d.writeInt(entries.size());

        entries.forEach((e)->{
            int l = e.length;
            try {
                d.writeInt(l);
                d.write(e);
            } catch(Exception ex) {
                LOG.error("Error serializing message ",ex);
                throw new RuntimeException("Error serializing message");
            }
        });


        byte[] getServerLogMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(getServerLogMsgArray.length+4);//

        retBuffer.putInt(getServerLogMsgArray.length);
        retBuffer.put(getServerLogMsgArray);

        retBuffer.flip() ; // make it ready for reading
        return retBuffer ;
    }

    public static GetServerLogResponse deserialize(ByteBuffer readBuffer) throws Exception {

        int messagesize = readBuffer.getInt() ;
        int type = readBuffer.getInt() ;
        if (type != messageType.value()) {

            throw new RuntimeException("Message is not the expected GetServerLogResponse") ;
        }

        int respId = readBuffer.getInt();
        int numEntries = readBuffer.getInt() ;

        ArrayList<byte[]> entries = new ArrayList<>();

        while(numEntries > 0) {

            int size = readBuffer.getInt();
            byte[] entry = new byte[size];
            readBuffer.get(entry, 0, size);
            entries.add(entry);
            --numEntries;
        }


        return new GetServerLogResponse(
                respId,
                entries) ;
    }

}
