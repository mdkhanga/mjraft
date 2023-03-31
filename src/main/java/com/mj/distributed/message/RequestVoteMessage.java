package com.mj.distributed.message;

import com.mj.distributed.model.LogEntryWithIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RequestVoteMessage implements Message {

    private static MessageType messageType = MessageType.RequestVote ;
    private int term;
    // private int candidateId;
    private String candidateHost ;
    private int candidatePort ;
    // private LogEntryWithIndex lastCommittedLogEntry ;
    private int lastLogIndex;
    private int lastLogTerm;

    private static Logger LOG  = LoggerFactory.getLogger(RequestVoteMessage.class) ;

    public RequestVoteMessage(int term,
                              String host,
                              int port,
                              int lastLogIndex,
                              int lastLogTerm) {
        this.term = term ;
        this.candidateHost = host;
        this.candidatePort = port;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term ;
    }

    public String getCandidateHost() { return candidateHost ;}

    public int getCandidatePort() { return candidatePort; }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }


    /**
     *
     *
     *
     */
    public ByteBuffer serialize() throws Exception {

        byte[] hostStringBytes = candidateHost.getBytes("UTF-8") ;

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType.value());
        d.writeInt(term);
        d.writeInt(hostStringBytes.length);
        d.write(hostStringBytes);
        d.writeInt(candidatePort);
        d.writeInt(lastLogIndex);
        d.writeInt(lastLogTerm);

        byte[] requestVoteMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(requestVoteMsgArray.length+4);//

        retBuffer.putInt(requestVoteMsgArray.length);
        retBuffer.put(requestVoteMsgArray);
        int l = requestVoteMsgArray.length+4 ;
        // LOG.info("request vote msg len = " + l) ;

        retBuffer.flip() ; // make it ready for reading

        return retBuffer ;
    }

    public static RequestVoteMessage deserialize(ByteBuffer readBuffer) throws Exception {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int type = readBuffer.getInt() ;
        if (type != messageType.value()) {

            throw new RuntimeException("Message is not the expected type RequestVote") ;
        }

        int term = readBuffer.getInt() ;

        int hostStringSize = readBuffer.getInt() ;
        byte[] hostStringBytes = new byte[hostStringSize] ;
        readBuffer.get(hostStringBytes,0,hostStringSize) ;
        String hostString = new String(hostStringBytes, "UTF-8") ;

        int port = readBuffer.getInt();
        int lastindex = readBuffer.getInt() ;

        int lastterm = readBuffer.getInt() ;

        return new RequestVoteMessage(
                term,
                hostString,
                port,
                lastindex,
                lastterm) ;
    }

}
