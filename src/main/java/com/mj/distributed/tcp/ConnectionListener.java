package com.mj.distributed.tcp;

import java.nio.ByteBuffer;

/**
 * Callbacks for one connection. All of them run on the connection's reader thread
 * (onClosed may also run on whichever thread calls close()),
 * so frames from one connection are delivered one at a time, in order.
 */
public interface ConnectionListener {

    default void onOpened(Connection c) {
    }

    void onFrame(Connection c, ByteBuffer frame);

    /** Called exactly once per connection. */
    default void onClosed(Connection c) {
    }
}
