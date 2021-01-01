package com.mj.distributed.message;

import com.mj.distributed.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RequestVoteMessage implements Message {

    private static MessageType messageType = MessageType.RequestVote ;
    private int term;
    private int candidateId;
    private String candidateHost ;
    private int candidatePort ;
    private LogEntry lastCommittedLogEntry ;

    private static Logger LOG  = LoggerFactory.getLogger(RequestVoteMessage.class) ;

    public RequestVoteMessage(int term,
                              int id,
                              String host,
                              int port,
                              LogEntry lastLogEntry) {

        this.term = term ;
        this.candidateId = candidateId ;
        this.candidateHost = host;
        this.candidatePort = port;
        this.lastCommittedLogEntry = lastLogEntry;
    }

    public int getTerm() {
        return term ;
    }

    public int getCandidateId() {
        return candidateId ;
    }

    public String getCandidateHost() { return candidateHost ;}

    public int getCandidatePort() { return candidatePort; }

    public LogEntry getCommittedLastLogEntry() { return lastCommittedLogEntry; }


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
        d.writeInt(candidateId);
        d.writeInt(hostStringBytes.length);
        d.write(hostStringBytes);
        d.writeInt(candidatePort);
        byte[] logEntryBytes = lastCommittedLogEntry.toBytes() ;
        d.writeInt(logEntryBytes.length);
        d.write(logEntryBytes);

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
        int candidateId = readBuffer.getInt();


        int hostStringSize = readBuffer.getInt() ;
        byte[] hostStringBytes = new byte[hostStringSize] ;
        readBuffer.get(hostStringBytes,0,hostStringSize) ;
        String hostString = new String(hostStringBytes, "UTF-8") ;

        int port = readBuffer.getInt() ;

        int logEntrySize = readBuffer.getInt() ;
        byte[] logEntryBytes = new byte[logEntrySize];
        readBuffer.get(logEntryBytes, 0, logEntrySize);

        LogEntry entry = LogEntry.fromBytes(logEntryBytes);

        return new RequestVoteMessage(
                term,
                candidateId,
                hostString,
                port,
                entry) ;
    }

}
