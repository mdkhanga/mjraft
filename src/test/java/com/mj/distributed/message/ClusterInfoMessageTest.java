package com.mj.distributed.message;

import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.model.Member;
import org.junit.jupiter.api.Test;


import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ClusterInfoMessageTest {

    @Test
    public void serialize() throws Exception {


        Member m = new Member("192.168.5.1",5050, true);
        ClusterInfo c = new ClusterInfo(m, new ArrayList<Member>());


        c.addMember(new Member("192.168.5.2",5051, false));
        c.addMember(new Member("192.168.5.3",5052, false));

        ClusterInfoMessage cm = new ClusterInfoMessage(c);
        ByteBuffer b = cm.serialize();

        ClusterInfoMessage cmCopy = ClusterInfoMessage.deserialize(b) ;

        assertEquals(5050, cmCopy.getClusterInfo().getLeader().getPort());

        System.out.println("done");
    }
}