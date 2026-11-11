package com.mj.distributed.tcp;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public interface Connection extends AutoCloseable {

    /**
     * Queues a frame for sending without blocking.
     * Returns false if the connection is closed. True means queued, not delivered.
     */
    boolean send(ByteBuffer frame);

    boolean isOpen();

    InetSocketAddress remoteAddress();

    /** Idempotent. Frames still queued are dropped. */
    @Override
    void close();
}
