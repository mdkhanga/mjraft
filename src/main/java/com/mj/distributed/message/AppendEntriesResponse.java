package com.mj.distributed.message;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

/**
 * Created by manoj on 6/18/17.
 */
public class AppendEntriesResponse implements Message {

    private static MessageType messageType = MessageType.AppendEntriesResponse ;
    private int seqOfMessageAcked ;
    private int term;
    private int success; // 1 = true, 0 = false

    public AppendEntriesResponse() {

    }

    public AppendEntriesResponse(int id, int term, boolean success) {
        this.seqOfMessageAcked= id ;
        this.term = term;
        this.success = success == true ? 1 : 0;

    }

    public AppendEntriesResponse(int id, int term, int s) {
        this.seqOfMessageAcked= id ;
        this.term = term;
        this.success = s;

    }

    public int getSeqOfMessageAcked() {
        return seqOfMessageAcked;
    }

    public boolean isSuccess() {
        return success == 1 ? true : false;
    }

    public int getTerm() {
        return term;
    }

    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType.value());
        d.writeInt(seqOfMessageAcked) ;
        d.writeByte(success);
        d.writeInt(term);

        byte[] ackArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(ackArray.length+4);//

        retBuffer.putInt(ackArray.length);
        retBuffer.put(ackArray);

        retBuffer.flip() ; // make it ready for reading

        return retBuffer ;
    }

    public static AppendEntriesResponse deserialize(ByteBuffer b) {

        int size = b.getInt() ;

        int type = b.getInt() ;

        if (type != messageType.value()) {
            throw new RuntimeException("Not a append Response message "+ type) ;
        }

        int seq = b.getInt() ;
        int success = (int)b.get();
        int term = b.getInt();

        AppendEntriesResponse r = new AppendEntriesResponse(seq, term, success) ;

        return r ;
    }
}
