package com.mj.distributed.tcp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FrameCodecTest {

    @Test
    void roundTrip() throws IOException {
        byte[] body = "hello".getBytes(UTF_8);

        ByteBuffer read = FrameCodec.readFrame(in(bytes(frame(body))));

        assertEquals(0, read.position());
        assertEquals(4 + body.length, read.remaining());
        assertArrayEquals(body, body(read));
    }

    @Test
    void twoFramesInOneRead() throws IOException {
        byte[] first = "first".getBytes(UTF_8);
        byte[] second = "second".getBytes(UTF_8);
        DataInputStream in = in(bytes(frame(first), frame(second)));

        assertArrayEquals(first, body(FrameCodec.readFrame(in)));
        assertArrayEquals(second, body(FrameCodec.readFrame(in)));
    }

    @Test
    void frameArrivingOneByteAtATime() throws IOException {
        byte[] body = "split across many reads".getBytes(UTF_8);
        InputStream oneByteAtATime = new FilterInputStream(new ByteArrayInputStream(bytes(frame(body)))) {
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return super.read(b, off, Math.min(1, len));
            }
        };

        ByteBuffer read = FrameCodec.readFrame(new DataInputStream(oneByteAtATime));

        assertArrayEquals(body, body(read));
    }

    @Test
    void largeFrame() throws IOException {
        byte[] body = new byte[1024 * 1024];
        new Random(42).nextBytes(body);

        ByteBuffer read = FrameCodec.readFrame(in(bytes(frame(body))));

        assertArrayEquals(body, body(read));
    }

    @Test
    void negativeLengthIsRejected() {
        byte[] data = ByteBuffer.allocate(4).putInt(-1).array();

        assertThrows(IOException.class, () -> FrameCodec.readFrame(in(data)));
    }

    @Test
    void oversizedLengthIsRejected() {
        byte[] data = ByteBuffer.allocate(4).putInt(FrameCodec.MAX_FRAME_BYTES + 1).array();

        assertThrows(IOException.class, () -> FrameCodec.readFrame(in(data)));
    }

    @Test
    void truncatedFrameThrowsEof() throws IOException {
        byte[] data = bytes(frame(new byte[10]));
        byte[] truncated = Arrays.copyOf(data, data.length - 3);

        assertThrows(EOFException.class, () -> FrameCodec.readFrame(in(truncated)));
    }

    @Test
    void emptyStreamThrowsEof() {
        assertThrows(EOFException.class, () -> FrameCodec.readFrame(in(new byte[0])));
    }

    @Test
    void writeRejectsMismatchedLengthPrefix() {
        ByteBuffer bad = ByteBuffer.allocate(8).putInt(99).putInt(0).flip();

        assertThrows(IllegalArgumentException.class,
                () -> FrameCodec.writeFrame(new ByteArrayOutputStream(), bad));
    }

    @Test
    void writeDoesNotMovePosition() throws IOException {
        ByteBuffer f = frame("x".getBytes(UTF_8));

        FrameCodec.writeFrame(new ByteArrayOutputStream(), f);

        assertEquals(0, f.position());
    }

    private static ByteBuffer frame(byte[] body) {
        return ByteBuffer.allocate(4 + body.length).putInt(body.length).put(body).flip();
    }

    private static byte[] bytes(ByteBuffer... frames) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (ByteBuffer f : frames) {
            FrameCodec.writeFrame(out, f);
        }
        return out.toByteArray();
    }

    private static DataInputStream in(byte[] data) {
        return new DataInputStream(new ByteArrayInputStream(data));
    }

    private static byte[] body(ByteBuffer frame) {
        frame.getInt();
        byte[] b = new byte[frame.remaining()];
        frame.get(b);
        return b;
    }
}
