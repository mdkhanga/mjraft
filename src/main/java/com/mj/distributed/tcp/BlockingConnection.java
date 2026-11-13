package com.mj.distributed.tcp;

import com.mj.distributed.message.Message;
import com.mj.distributed.message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Synchronous request/response over a socket, for clients.
 * The caller's thread does the I/O: no reader or writer threads, no wait/notify.
 *
 * A ReentrantLock rather than synchronized, so a virtual thread blocked on the socket does not pin its carrier.
 */
public final class BlockingConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingConnection.class);

    private final Socket socket;
    private final DataInputStream in;
    private final OutputStream out;
    private final ReentrantLock lock = new ReentrantLock();

    private BlockingConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new BufferedOutputStream(socket.getOutputStream());
    }

    /**
     * @param responseTimeout how long call() waits for a reply before throwing SocketTimeoutException
     */
    public static BlockingConnection open(String host, int port, Duration connectTimeout, Duration responseTimeout)
            throws IOException {
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), (int) connectTimeout.toMillis());
            socket.setSoTimeout((int) responseTimeout.toMillis());
            return new BlockingConnection(socket);
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    /**
     * Sends without waiting for a reply.
     */
    public void send(Message m) throws Exception {
        lock.lock();
        try {
            write(m);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sends a request and returns the first reply of one of the expected types, positioned at 0.
     * Frames of other types are logged and skipped.
     *
     * @throws java.net.SocketTimeoutException if no expected reply arrives within the response timeout
     */
    public ByteBuffer call(Message request, MessageType... expected) throws Exception {
        Set<MessageType> types = Set.of(expected);
        lock.lock();
        try {
            write(request);
            while (true) {
                ByteBuffer frame = FrameCodec.readFrame(in);
                MessageType type = MessageType.valueOf(frame.getInt(4));
                if (types.contains(type)) {
                    return frame;
                }
                LOG.info("Skipping unexpected " + type + " message from " + socket.getRemoteSocketAddress());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    private void write(Message m) throws Exception {
        FrameCodec.writeFrame(out, m.serialize());
        out.flush();
    }
}
