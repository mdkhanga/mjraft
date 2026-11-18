package com.mj.raft.storage;

import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Stores the term, vote and log in two files in a data directory, and flushes every write to disk before returning.
 * <p>
 * {@code state}: magic, version, term, vote, CRC32. It is replaced as a whole by writing {@code state.tmp} and
 * renaming it, so a crash leaves either the old state or the new one.
 * <p>
 * {@code log}: magic and version, then one record per entry: length, CRC32, term, data. The length counts the
 * term and data, and the CRC covers them. Records are only appended, or cut off by {@link #truncateFrom}.
 * A crash during an append can leave an incomplete last record; {@link #open} drops it. Damage anywhere
 * else is reported as {@link CorruptStorageException}, because dropping it could lose committed entries.
 */
public final class FileRaftStorage implements RaftStorage {

    private static final Logger LOG = LoggerFactory.getLogger(FileRaftStorage.class);

    static final String STATE_FILE = "state";
    static final String STATE_TMP_FILE = "state.tmp";
    static final String LOG_FILE = "log";

    private static final int STATE_MAGIC = 0x4d4a5253; // "MJRS"
    private static final int LOG_MAGIC = 0x4d4a524c;   // "MJRL"
    private static final int VERSION = 1;

    static final int LOG_HEADER_BYTES = 8;    // magic, version
    static final int RECORD_HEADER_BYTES = 8; // length, CRC32

    private final Path dir;
    private final FileChannel log;
    private final ReentrantLock lock = new ReentrantLock();
    // file position of each entry's record, by log index
    private final List<Long> offsets = new ArrayList<>();
    // file position just after the last record
    private long end;

    private FileRaftStorage(Path dir, FileChannel log) {
        this.dir = dir;
        this.log = log;
    }

    /**
     * Opens the storage in dir, creating it if needed, and drops an incomplete last record left by a crash.
     *
     * @throws CorruptStorageException if the stored data is damaged in a way a crash cannot explain
     */
    public static FileRaftStorage open(Path dir) throws IOException {
        Files.createDirectories(dir);
        // left by a crash before the rename; the state file is still intact
        Files.deleteIfExists(dir.resolve(STATE_TMP_FILE));

        FileChannel log = FileChannel.open(dir.resolve(LOG_FILE), CREATE, READ, WRITE);
        try {
            FileRaftStorage storage = new FileRaftStorage(dir, log);
            storage.recover();
            readState(dir); // so a damaged state file fails here rather than on first use
            return storage;
        } catch (IOException | RuntimeException e) {
            log.close();
            throw e;
        }
    }

    private void recover() throws IOException {
        if (log.size() < LOG_HEADER_BYTES) {
            // new, or a crash while writing the header, so there can be no records
            ByteBuffer header = ByteBuffer.allocate(LOG_HEADER_BYTES).putInt(LOG_MAGIC).putInt(VERSION).flip();
            log.truncate(0);
            writeFully(log, header, 0);
            log.force(true);
            syncDirectory(dir);
        } else {
            ByteBuffer header = ByteBuffer.allocate(LOG_HEADER_BYTES);
            readFully(header, 0);
            header.flip();
            int magic = header.getInt();
            int version = header.getInt();
            if (magic != LOG_MAGIC || version != VERSION) {
                throw new CorruptStorageException(dir.resolve(LOG_FILE) + " is not a version " + VERSION + " log file");
            }
        }

        Scan scan = scan();
        if (scan.torn()) {
            LOG.warn("Dropping an incomplete record at offset " + scan.end() + " of " + dir.resolve(LOG_FILE)
                    + ", left by an interrupted write");
            log.truncate(scan.end());
            log.force(true);
        }
        offsets.addAll(scan.offsets());
        end = scan.end();
    }

    @Override
    public PersistentState load() throws IOException {
        lock.lock();
        try {
            State state = readState(dir);
            return new PersistentState(state.term(), state.votedFor(), scan().entries());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void saveTermAndVote(int term, Member votedFor) throws IOException {
        lock.lock();
        try {
            Path tmp = dir.resolve(STATE_TMP_FILE);
            try (FileChannel c = FileChannel.open(tmp, CREATE, WRITE, TRUNCATE_EXISTING)) {
                writeFully(c, encodeState(term, votedFor), 0);
                c.force(true);
            }
            Files.move(tmp, dir.resolve(STATE_FILE), ATOMIC_MOVE);
            syncDirectory(dir);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void append(List<LogEntry> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }
        int bytes = 0;
        for (LogEntry e : entries) {
            bytes += RECORD_HEADER_BYTES + Integer.BYTES + e.getEntry().length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(bytes);
        List<Long> newOffsets = new ArrayList<>(entries.size());

        lock.lock();
        try {
            long pos = end;
            for (LogEntry e : entries) {
                newOffsets.add(pos);
                int start = buffer.position();
                int length = Integer.BYTES + e.getEntry().length;
                buffer.putInt(length).putInt(0).putInt(e.getTerm()).put(e.getEntry());
                buffer.putInt(start + Integer.BYTES, crc32(buffer.array(), start + RECORD_HEADER_BYTES, length));
                pos += RECORD_HEADER_BYTES + length;
            }
            buffer.flip();
            try {
                writeFully(log, buffer, end);
                log.force(true);
            } catch (IOException e) {
                // don't leave part of the batch behind for the next append to land after
                log.truncate(end);
                throw e;
            }
            offsets.addAll(newOffsets);
            end = pos;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void truncateFrom(int index) throws IOException {
        lock.lock();
        try {
            Objects.checkIndex(index, offsets.size() + 1);
            if (index == offsets.size()) {
                return;
            }
            long pos = offsets.get(index);
            log.truncate(pos);
            log.force(true);
            offsets.subList(index, offsets.size()).clear();
            end = pos;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            log.close();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param torn true if the bytes from end onwards are an incomplete record
     */
    private record Scan(List<LogEntry> entries, List<Long> offsets, long end, boolean torn) {
    }

    private Scan scan() throws IOException {
        long size = log.size();
        long pos = LOG_HEADER_BYTES;
        List<LogEntry> entries = new ArrayList<>();
        List<Long> offsets = new ArrayList<>();
        ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_BYTES);

        while (pos < size) {
            if (size - pos < RECORD_HEADER_BYTES) {
                return new Scan(entries, offsets, pos, true);
            }
            header.clear();
            readFully(header, pos);
            header.flip();
            int length = header.getInt();
            int crc = header.getInt();
            long recordEnd = pos + RECORD_HEADER_BYTES + length;
            if (length < Integer.BYTES || recordEnd > size) {
                // Runs past the end of the file, or has a length no record can have, as when a crash
                // leaves zeros behind: the tail of an interrupted append. A damaged length looks the same.
                return new Scan(entries, offsets, pos, true);
            }

            ByteBuffer body = ByteBuffer.allocate(length);
            readFully(body, pos + RECORD_HEADER_BYTES);
            if (crc32(body.array(), 0, length) != crc) {
                if (recordEnd == size) {
                    return new Scan(entries, offsets, pos, true);
                }
                throw new CorruptStorageException("Log record " + entries.size() + " at offset " + pos + " of "
                        + dir.resolve(LOG_FILE) + " fails its checksum, and more records follow it");
            }
            body.flip();
            int term = body.getInt();
            byte[] data = new byte[body.remaining()];
            body.get(data);
            entries.add(new LogEntry(term, data));
            offsets.add(pos);
            pos = recordEnd;
        }
        return new Scan(entries, offsets, pos, false);
    }

    private record State(int term, Member votedFor) {
    }

    private static ByteBuffer encodeState(int term, Member votedFor) {
        byte[] host = votedFor == null ? null : votedFor.getHostString().getBytes(StandardCharsets.UTF_8);
        int bytes = 4 * Integer.BYTES + 1 + (host == null ? 0 : 2 * Integer.BYTES + host.length);
        ByteBuffer b = ByteBuffer.allocate(bytes);
        b.putInt(STATE_MAGIC).putInt(VERSION).putInt(term);
        if (host == null) {
            b.put((byte) 0);
        } else {
            b.put((byte) 1).putInt(host.length).put(host).putInt(votedFor.getPort());
        }
        b.putInt(crc32(b.array(), 0, b.position()));
        return b.flip();
    }

    private static State readState(Path dir) throws IOException {
        Path path = dir.resolve(STATE_FILE);
        if (!Files.exists(path)) {
            return new State(0, null);
        }
        byte[] bytes = Files.readAllBytes(path);
        try {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            int crcStart = bytes.length - Integer.BYTES;
            if (crcStart < 0 || b.getInt(crcStart) != crc32(bytes, 0, crcStart)) {
                throw new CorruptStorageException(path + " fails its checksum");
            }
            if (b.getInt() != STATE_MAGIC || b.getInt() != VERSION) {
                throw new CorruptStorageException(path + " is not a version " + VERSION + " state file");
            }
            int term = b.getInt();
            Member votedFor = null;
            if (b.get() == 1) {
                byte[] host = new byte[b.getInt()];
                b.get(host);
                votedFor = new Member(new String(host, StandardCharsets.UTF_8), b.getInt());
            }
            return new State(term, votedFor);
        } catch (BufferUnderflowException | IndexOutOfBoundsException | NegativeArraySizeException e) {
            throw new CorruptStorageException(path + " is truncated or malformed");
        }
    }

    private static int crc32(byte[] bytes, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    private void readFully(ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int n = log.read(buffer, position);
            if (n < 0) {
                throw new EOFException("Unexpected end of " + dir.resolve(LOG_FILE));
            }
            position += n;
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            position += channel.write(buffer, position);
        }
    }

    /**
     * Makes a file creation or rename in dir durable. Some platforms cannot open a directory; there it is skipped.
     */
    private static void syncDirectory(Path dir) {
        try (FileChannel c = FileChannel.open(dir, READ)) {
            c.force(true);
        } catch (IOException e) {
            LOG.debug("Cannot sync directory " + dir, e);
        }
    }
}
