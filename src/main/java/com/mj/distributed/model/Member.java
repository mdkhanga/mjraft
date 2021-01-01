package com.mj.distributed.model;

import java.io.*;
import java.util.Objects;

public class Member {

    private final String hostString ;
    private final int port ;
    private final boolean leader ;

    public Member( String h, int p, boolean leader) {

        this.hostString = h ;
        this.port = p ;
        this.leader = leader ;
    }

    public Member( String h, int p) {

        this.hostString = h ;
        this.port = p ;
        this.leader = false;
    }

    public String getHostString() {
        return hostString;
    }

    public int getPort() {
        return port ;
    }

    public boolean isLeader() {
        return leader;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(out);

        byte[] hostStringBytes = hostString.getBytes("UTF-8");

        d.writeInt(hostStringBytes.length);
        d.write(hostStringBytes);
        d.writeInt(port);
        d.writeBoolean(leader);
        byte[] ret = out.toByteArray();
        out.close();
        d.close();
        return ret;
    }

    public static Member fromBytes(byte[] bytes) throws IOException {

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);
        int hostStringSize = din.readInt() ;
        byte[] hostStringBytes = new byte[hostStringSize] ;
        din.read(hostStringBytes,0, hostStringSize) ;
        String hostString = new String(hostStringBytes);
        int port = din.readInt();
        boolean l = din.readBoolean();

        return new Member(hostString, port, l);

    }

    public String toString() {
        StringBuilder b = new StringBuilder() ;
        b.append("[");
        b.append(hostString);
        b.append(",");
        b.append(port);
        b.append(",");
        b.append(leader);
        b.append("]");
        return b.toString();

    }

    @Override
    public boolean equals(Object obj) {

        if (obj == this) return true ;

        if (! (obj instanceof Member)) {
            return false ;
        }
        Member m = (Member) obj ;

        return hostString.equals(m.hostString) &&
                port == m.port ;

    }

    @Override
    public int hashCode() {
        return Objects.hash(hostString, port);
    }
}
