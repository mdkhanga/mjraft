package com.mj.distributed.message;

import com.mj.distributed.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class GetServerLog implements Message {

    private static MessageType messageType = MessageType.GetServerLog ;

    private final int seqId;
    private final int startIndex;
    private final int count;
    private final byte includeUnComitted  ; // 0 == false , 1 == true


    private static Logger LOG  = LoggerFactory.getLogger(GetServerLog.class) ;

    public GetServerLog(int id,
                        int startIndex,
                        int count) {

        this.seqId = id;
        this.startIndex = startIndex ;
        this.count = count;
        includeUnComitted = 0;
    }

    public GetServerLog(int id,
                        int startIndex,
                        int count,
                        byte includeUnComitted) {
        this.seqId = id;
        this.startIndex = startIndex ;
        this.count = count;
        this.includeUnComitted = includeUnComitted;
    }

    public int getSeqId() {
        return seqId;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public int getCount() {
        return count;
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
        d.writeInt(seqId);
        d.writeInt(startIndex);
        d.writeInt(count);
        d.writeByte(includeUnComitted);


        byte[] getVoteMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(getVoteMsgArray.length+4);//

        retBuffer.putInt(getVoteMsgArray.length);
        retBuffer.put(getVoteMsgArray);

        retBuffer.flip() ; // make it ready for reading
        return retBuffer ;
    }

    public static GetServerLog deserialize(ByteBuffer readBuffer) throws Exception {

        int messagesize = readBuffer.getInt() ;
        int type = readBuffer.getInt() ;
        if (type != messageType.value()) {

            throw new RuntimeException("Message is not the expected type GetServerLog") ;
        }

        int id = readBuffer.getInt();
        int startIndex = readBuffer.getInt() ;
        int count = readBuffer.getInt();
        byte includeCommitted = readBuffer.get() ;

        return new GetServerLog(
                id,
                startIndex,
                count,
                includeCommitted
        ) ;
    }

}
