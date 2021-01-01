package com.mj.distributed.model;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ClusterInfo {

    private final Member leader ;
    private final List<Member> members ;



    public ClusterInfo(Member leader, List<Member> ms) {
        this.leader = leader;
        this.members = ms ;
        members.add(leader);
    }

    public void addMember(Member m) {
        members.add(m);
    }

    public Member getLeader() {
        return leader;
    }

    public List<Member> getMembers() {
        return members;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(out);

        byte[] leaderBytes = leader.toBytes();
        d.writeInt(leaderBytes.length);
        d.write(leaderBytes);

        d.writeInt(members.size());

        for (int i = 0 ; i < members.size() ; i++) {
            byte[] mbytes = members.get(i).toBytes() ;
            d.writeInt(mbytes.length);
            d.write(mbytes);
        }

        byte[] ret = out.toByteArray();
        out.close();
        d.close();
        return ret;
    }

    public static ClusterInfo fromBytes(byte[] bytes) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);

        int numleaderBytes = din.readInt();
        byte[] leaderbytes = new byte[numleaderBytes];
        din.read(leaderbytes, 0, numleaderBytes);
        Member leader = Member.fromBytes(leaderbytes);

        ClusterInfo cinfo = new ClusterInfo(leader, new ArrayList<Member>());

        int numMembers = din.readInt() ;

        for(int i = 0 ; i < numMembers ; i++) {

            int numbytes = din.readInt() ;
            byte[] mbytes = new byte[numbytes];
            din.read(mbytes, 0, numbytes);
            Member m = Member.fromBytes(mbytes);
            cinfo.addMember(m);

            /* if (m.isLeader()) {
                cinfo.setLeader(m);
            } */


        }


        return cinfo;
    }

    public void removeMember(String host, int port) {

        Iterator<Member> itm = members.iterator();

        while (itm.hasNext()) {
            Member m = itm.next();
            if (m.getHostString().equals(host) && m.getPort() == port ) {
                itm.remove();
            }
        }
    }

    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[");
        members.forEach((m)->{
           b.append(m.toString());
        });
        b.append("]");

        return b.toString();
    }
}
