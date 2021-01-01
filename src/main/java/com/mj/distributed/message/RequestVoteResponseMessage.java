package com.mj.distributed.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RequestVoteResponseMessage implements Message {

    private static MessageType messageType = MessageType.RequestVoteResponse ;
    private int term ;
    private int candidateId ;
    private boolean vote ; // true = yes

    private static Logger LOG  = LoggerFactory.getLogger(RequestVoteResponseMessage.class) ;

    public RequestVoteResponseMessage(int term, int candidateId, boolean vote) {

        this.term = term ;
        this.candidateId = candidateId ;
        this.vote = vote ;

    }

    public int getTerm() {
        return term ;
    }

    public int getCandidateId() {
        return candidateId ;
    }

    public boolean getVote() { return vote ;}


    /**
     *
     *
     *
     */
    public ByteBuffer serialize() throws Exception {


        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType.value());
        d.writeInt(term);
        d.writeInt(candidateId);
        if (vote) {
            d.writeByte(1);
        } else {
            d.writeByte(0);
        }

        byte[] voteResponseMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(voteResponseMsgArray.length+4);//

        retBuffer.putInt(voteResponseMsgArray.length);
        retBuffer.put(voteResponseMsgArray);

        retBuffer.flip() ; // make it ready for reading

        return retBuffer ;
    }

    public static RequestVoteResponseMessage deserialize(ByteBuffer readBuffer) {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int type = readBuffer.getInt() ;
        if (type != messageType.value()) {

            throw new RuntimeException("Message is not the expected type ReequestVoteResponseMessage") ;
        }

        int term = readBuffer.getInt() ;
        int candidateId = readBuffer.getInt();
        int b = readBuffer.get() ;
        boolean vote = false ;
        if ( b == 1) {
            vote = true ;
        }

        return new RequestVoteResponseMessage(term, candidateId, vote) ;
    }

}
