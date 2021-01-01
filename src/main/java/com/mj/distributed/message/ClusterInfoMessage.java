package com.mj.distributed.message;

import com.mj.distributed.model.ClusterInfo;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class ClusterInfoMessage implements Message {

    private static MessageType messageType = MessageType.ClusterInfo ;

    private ClusterInfo clusterInfo;

    public ClusterInfoMessage(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);

        d.writeInt(messageType.value());
        byte[] clusterinfobytes = clusterInfo.toBytes();
        d.writeInt(clusterinfobytes.length);
        d.write(clusterinfobytes);

        byte[] bytestoWrite = b.toByteArray();
        ByteBuffer ret = ByteBuffer.allocate(bytestoWrite.length+4);
        ret.putInt(bytestoWrite.length);
        ret.put(bytestoWrite);
        ret.flip();
        return ret ;

    }

    public static ClusterInfoMessage deserialize(ByteBuffer b) throws Exception {
        int messagesize = b.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int type = b.getInt() ;
        if (messageType.value() != type) {
            throw new RuntimeException("Message is not the expected type ClusterInfoMessage") ;
        }

        int clusterinfoSize = b.getInt();
        byte[] clusterbytes = new byte[clusterinfoSize];
        b = b.get(clusterbytes, 0, clusterinfoSize);

        return new ClusterInfoMessage(ClusterInfo.fromBytes(clusterbytes));

    }

    public String toString() {
        return clusterInfo.toString();
    }
}
