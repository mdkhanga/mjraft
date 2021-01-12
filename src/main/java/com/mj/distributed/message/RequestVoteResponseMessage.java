package com.mj.distributed.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RequestVoteResponseMessage implements Message {

    private static MessageType messageType = MessageType.RequestVoteResponse ;
    private int term ;
    private String candidateHost ;
    private int candidatePort;
    private boolean vote ; // true = yes

    private static Logger LOG  = LoggerFactory.getLogger(RequestVoteResponseMessage.class) ;

    public RequestVoteResponseMessage(int term, String candidateHost, int candidatePort, boolean vote) {

        this.term = term ;
        this.candidateHost = candidateHost ;
        this.candidatePort = candidatePort;
        this.vote = vote ;

    }

    public int getTerm() {
        return term ;
    }

    /* public int getCandidateId() {
        return candidateId ;
    } */

    public int getCandidatePort() {
        return candidatePort;
    }

    public String getCandidateHost() {
        return candidateHost ;
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

        byte[] hostStringBytes = candidateHost.getBytes("UTF-8") ;
        d.writeInt(hostStringBytes.length);
        d.write(hostStringBytes);

        d.writeInt(candidatePort);
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

    public static RequestVoteResponseMessage deserialize(ByteBuffer readBuffer) throws Exception {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int type = readBuffer.getInt() ;
        if (type != messageType.value()) {

            throw new RuntimeException("Message is not the expected type ReequestVoteResponseMessage") ;
        }

        int term = readBuffer.getInt() ;
        // int candidateId = readBuffer.getInt();
        int hostStringSize = readBuffer.getInt() ;
        byte[] hostStringBytes = new byte[hostStringSize] ;
        readBuffer.get(hostStringBytes,0,hostStringSize) ;
        String hostString = new String(hostStringBytes, "UTF-8") ;

        int port = readBuffer.getInt() ;


        int b = readBuffer.get() ;
        boolean vote = false ;
        if ( b == 1) {
            vote = true ;
        }

        return new RequestVoteResponseMessage(term, hostString, port, vote) ;
    }

}
