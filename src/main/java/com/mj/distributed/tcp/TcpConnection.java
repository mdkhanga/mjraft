package com.mj.distributed.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A socket served by two virtual threads: a reader that delivers frames to the listener in order,
 * and a writer that drains the outbound queue so send() never blocks.
 */
public final class TcpConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(TcpConnection.class);

    // tells the writer thread to exit
    private static final ByteBuffer CLOSE_SIGNAL = ByteBuffer.allocate(0);

    private final Socket socket;
    private final ConnectionListener listener;
    private final InetSocketAddress remote;
    private final BlockingQueue<ByteBuffer> outbound = new LinkedBlockingQueue<>();
    private final AtomicBoolean open = new AtomicBoolean(true);

    TcpConnection(Socket socket, ConnectionListener listener) throws IOException {
        this.socket = socket;
        this.listener = listener;
        this.remote = (InetSocketAddress) socket.getRemoteSocketAddress();
        socket.setTcpNoDelay(true);
    }

    void start() {
        listener.onOpened(this);
        Thread.ofVirtual().name("tcp-reader-" + remote).start(this::readLoop);
        Thread.ofVirtual().name("tcp-writer-" + remote).start(this::writeLoop);
    }

    @Override
    public boolean send(ByteBuffer frame) {
        if (!open.get()) {
            return false;
        }
        outbound.add(frame);
        return true;
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return remote;
    }

    @Override
    public void close() {
        if (!open.compareAndSet(true, false)) {
            return;
        }
        outbound.clear();
        outbound.add(CLOSE_SIGNAL);
        try {
            socket.close();
        } catch (IOException e) {
            LOG.debug("Error closing socket to " + remote, e);
        }
        try {
            listener.onClosed(this);
        } catch (RuntimeException e) {
            LOG.error("Error in onClosed for " + remote, e);
        }
    }

    private void readLoop() {
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            while (true) {
                ByteBuffer frame = FrameCodec.readFrame(in);
                try {
                    listener.onFrame(this, frame);
                } catch (RuntimeException e) {
                    // one bad message must not kill the connection
                    LOG.error("Error handling frame from " + remote, e);
                }
            }
        } catch (EOFException e) {
            LOG.debug(remote + " closed the connection");
        } catch (IOException e) {
            if (open.get()) {
                LOG.info("Read from " + remote + " failed: " + e.getMessage());
            }
        } finally {
            close();
        }
    }

    private void writeLoop() {
        try {
            OutputStream out = socket.getOutputStream();
            while (true) {
                ByteBuffer frame = outbound.take();
                if (frame == CLOSE_SIGNAL) {
                    return;
                }
                FrameCodec.writeFrame(out, frame);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            if (open.get()) {
                LOG.info("Write to " + remote + " failed: " + e.getMessage());
            }
        } finally {
            close();
        }
    }

    @Override
    public String toString() {
        return "TcpConnection[" + remote + "]";
    }
}
