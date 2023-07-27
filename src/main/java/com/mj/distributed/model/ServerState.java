package com.mj.distributed.model;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ServerState {
    private int currentTerm;
    private int lastCommittedIndex;
    private int lastpersistedIndex;
    private Member votedFor;

    public ServerState(int c, int lc, int lp, Member m) {
        currentTerm = c;
        lastCommittedIndex = lc;
        lastpersistedIndex = lp;
        votedFor = m ;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int t) {
        currentTerm = t;
    }

    public int getLastCommittedIndex() {
        return lastCommittedIndex;
    }

    public void setLastCommittedIndex(int lci) {
        lastCommittedIndex = lci;
    }

    public int getLastpersistedIndex() {
        return lastpersistedIndex;
    }

    public void setLastpersistedIndex(int lpi) {
        lastpersistedIndex = lpi;
    }

    public Member getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(Member s) {
        votedFor = s;
    }

    public ByteBuffer serialize() throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);

        d.writeInt(currentTerm);
        d.writeInt(lastCommittedIndex);
        d.writeInt(lastpersistedIndex);

        byte[] mbytes = votedFor.toBytes();
        d.writeInt(mbytes.length);
        d.write(mbytes);

        byte[] bytestoWrite = b.toByteArray();
        ByteBuffer ret = ByteBuffer.allocate(bytestoWrite.length+4);
        ret.putInt(bytestoWrite.length);
        ret.put(bytestoWrite);
        ret.flip();
        return ret ;
    }

    public static ServerState deserialize(ByteBuffer b) throws IOException {

        int cTerm = b.getInt();
        int lcIndex = b.getInt();
        int lpIndex = b.getInt();
        int vSize = b.getInt();

        byte[] vBytes = new byte[vSize];
        b.get(vBytes, 0, vSize);
        Member vFor = Member.fromBytes(vBytes);

        return new ServerState(cTerm, lcIndex, lpIndex, vFor);
    }
}
