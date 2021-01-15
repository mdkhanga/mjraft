package com.mj.distributed.message;

import com.mj.distributed.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AppendEntriesMessage implements Message {

    private static MessageType messageType = MessageType.AppendEntries;

    private String leaderId ;

    private int term = 0 ;
    private List<LogEntry> entries = new ArrayList<>();

    private int prevTerm = -1 ;
    private int prevIndex = -1 ;

    private int leaderCommitIndex = -1;

    private int seqId ;

    private static Logger LOG  = LoggerFactory.getLogger(AppendEntriesMessage.class);

    public AppendEntriesMessage(String leaderId, int seqId) {
        this.leaderId = leaderId;
        this.seqId = seqId;
    }

    public AppendEntriesMessage(int term, String leaderId, int seqId, int prevIndex, int leaderCommitIndex) {

        this.term = term;
        this.leaderId = leaderId;
        this.seqId = seqId;
        this.prevIndex = prevIndex;
        this.leaderCommitIndex = leaderCommitIndex;
    }

    public void addLogEntry(LogEntry e) {
        entries.add(e);
    }

    public List<LogEntry> getLogEntries() {
        return entries;
    }

    public LogEntry getLogEntry() {
        return entries.size() == 1 ? entries.get(0) : null;
    }

    public int getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public int getSeqId() {
        return seqId;
    }

    public int getPrevIndex() {
        return prevIndex ;
    }

    public void setPrevIndex(int i) {
        prevIndex = i;
    }

    public int getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);

        d.writeInt(messageType.value());
        d.writeInt(term);

        // d.writeInt(leaderId);
        byte[] leaderBytes = leaderId.getBytes("UTF-8");
        d.writeInt(leaderBytes.length);
        d.write(leaderBytes);
        d.writeInt(seqId);
        d.writeInt(prevIndex);
        d.writeInt(leaderCommitIndex);

        // LOG.info("Ser Entries size "+ entries.size());
        d.writeInt(entries.size());
        if (entries.size() > 0) {

            // d.writeInt(entries.size());

            entries.forEach((e)->{
                try {
                    byte[] ebytes = e.toBytes();
                    d.writeInt(ebytes.length);
                    d.write(ebytes);
                } catch(Exception e1) {
                    throw new RuntimeException(e1);
                }
            });
        }

        byte[] bytestoWrite = b.toByteArray();
        ByteBuffer ret = ByteBuffer.allocate(bytestoWrite.length+4);
        ret.putInt(bytestoWrite.length);
        ret.put(bytestoWrite);
        ret.flip();
        return ret ;
    }

    public static AppendEntriesMessage deserialize(ByteBuffer b) throws Exception {
        int messagesize = b.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int type = b.getInt() ;
        if (messageType.value() != type) {
            throw new RuntimeException("Message is not the expected type AppendEntriesMessage") ;
        }

        int term = b.getInt();
        // int leaderId = b.getInt();
        int leaderSize = b.getInt();
        byte[] leaderBytes = new byte[leaderSize];
        b.get(leaderBytes, 0, leaderSize);
        String leaderId = new String(leaderBytes);
        int seqId = b.getInt();
        int prevIndex = b.getInt();
        int leaderCommitIndex = b.getInt();

        AppendEntriesMessage newMsg = new AppendEntriesMessage(term, leaderId, seqId, prevIndex, leaderCommitIndex);

        int numEntries = b.getInt() ;
       while (numEntries > 0) {
            int size = b.getInt();
            byte[] entrybytes = new byte[size];
            b = b.get(entrybytes, 0, size);
            newMsg.addLogEntry(LogEntry.fromBytes(entrybytes));
            --numEntries;
        }

        return newMsg ;
    }
}
