package com.mj.distributed.tcp;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Length-prefixed framing: [int length][length bytes].
 * Message.serialize() already produces exactly this layout, so frames are passed through unchanged.
 */
public final class FrameCodec {

    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    private FrameCodec() {
    }

    /**
     * Blocks until one whole frame has been read.
     * The returned buffer holds [length][body] positioned at 0, which is what the message deserialize() methods expect.
     *
     * @throws java.io.EOFException if the stream ends, including cleanly between frames
     */
    public static ByteBuffer readFrame(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_FRAME_BYTES) {
            throw new IOException("Invalid frame length: " + length);
        }
        ByteBuffer frame = ByteBuffer.allocate(4 + length);
        frame.putInt(length);
        in.readFully(frame.array(), 4, length);
        frame.rewind();
        return frame;
    }

    /**
     * Writes the frame between its position and limit. Does not change the buffer's position.
     */
    public static void writeFrame(OutputStream out, ByteBuffer frame) throws IOException {
        int remaining = frame.remaining();
        if (remaining < 4 || frame.getInt(frame.position()) != remaining - 4) {
            throw new IllegalArgumentException("Buffer is not a single length-prefixed frame");
        }
        if (frame.hasArray()) {
            out.write(frame.array(), frame.arrayOffset() + frame.position(), remaining);
        } else {
            byte[] copy = new byte[remaining];
            frame.duplicate().get(copy);
            out.write(copy);
        }
    }
}
