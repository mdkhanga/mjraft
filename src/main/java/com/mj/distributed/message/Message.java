package com.mj.distributed.message;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface Message {
    public ByteBuffer serialize() throws Exception;
}
