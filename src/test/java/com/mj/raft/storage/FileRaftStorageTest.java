package com.mj.raft.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The shared contract, plus what only files have: surviving a reopen, and recovering from a crash or damage.
 * Crashes are simulated by editing the files while the storage is closed.
 */
class FileRaftStorageTest extends RaftStorageContractTest {

    // the first data byte of the first record: log header, record header, term
    private static final int FIRST_DATA_BYTE = FileRaftStorage.LOG_HEADER_BYTES + FileRaftStorage.RECORD_HEADER_BYTES + Integer.BYTES;

    @TempDir
    Path dir;

    @Override
    RaftStorage open() throws IOException {
        return FileRaftStorage.open(dir);
    }

    private PersistentState reopenAndLoad() throws IOException {
        storage.close();
        storage = open();
        return storage.load();
    }

    private Path logFile() {
        return dir.resolve(FileRaftStorage.LOG_FILE);
    }

    @Test
    void everythingSurvivesReopen() throws IOException {
        storage.saveTermAndVote(5, A);
        storage.append(List.of(entry(4, "a"), entry(5, "b")));

        PersistentState s = reopenAndLoad();

        assertEquals(5, s.term());
        assertEquals(A, s.votedFor());
        assertLog(s.log(), entry(4, "a"), entry(5, "b"));
    }

    @Test
    void truncateSurvivesReopen() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "b")));
        storage.truncateFrom(1);

        assertLog(reopenAndLoad().log(), entry(1, "a"));
    }

    @Test
    void incompleteLastRecordIsDropped() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "bbbb")));
        storage.close();
        chop(logFile(), 3); // a crash part way through writing "bbbb"

        assertLog(reopenAndLoad().log(), entry(1, "a"));
    }

    @Test
    void incompleteLastRecordHeaderIsDropped() throws IOException {
        storage.append(List.of(entry(1, "a")));
        storage.close();
        appendBytes(logFile(), new byte[]{0, 0, 0, 9, 1});

        assertLog(reopenAndLoad().log(), entry(1, "a"));
    }

    @Test
    void zeroFilledTailIsDropped() throws IOException {
        storage.append(List.of(entry(1, "a")));
        storage.close();
        appendBytes(logFile(), new byte[64]); // some file systems extend the file before the data reaches disk

        assertLog(reopenAndLoad().log(), entry(1, "a"));
    }

    @Test
    void lastRecordWithBadChecksumIsDropped() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "b")));
        storage.close();
        flipByte(logFile(), Files.size(logFile()) - 1);

        assertLog(reopenAndLoad().log(), entry(1, "a"));
    }

    @Test
    void appendsContinueAfterDroppedRecord() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "bbbb")));
        storage.close();
        chop(logFile(), 3);
        reopenAndLoad();

        storage.append(List.of(entry(2, "c")));

        assertLog(reopenAndLoad().log(), entry(1, "a"), entry(2, "c"));
    }

    @Test
    void badChecksumBeforeTheLastRecordFailsOpen() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "b")));
        storage.close();
        flipByte(logFile(), FIRST_DATA_BYTE);

        assertThrows(CorruptStorageException.class, this::open);
    }

    @Test
    void damagedStateFileFailsOpen() throws IOException {
        storage.saveTermAndVote(2, A);
        storage.close();
        flipByte(dir.resolve(FileRaftStorage.STATE_FILE), 9);

        assertThrows(CorruptStorageException.class, this::open);
    }

    @Test
    void fileThatIsNotALogFailsOpen() throws IOException {
        storage.close();
        Files.writeString(logFile(), "not a raft log");

        assertThrows(CorruptStorageException.class, this::open);
    }

    @Test
    void leftoverTempStateFileIsIgnored() throws IOException {
        storage.saveTermAndVote(2, A);
        storage.close();
        // a crash after writing the temp file but before the rename
        Files.writeString(dir.resolve(FileRaftStorage.STATE_TMP_FILE), "half written");

        PersistentState s = reopenAndLoad();

        assertEquals(2, s.term());
        assertEquals(A, s.votedFor());
    }

    private static void chop(Path file, int bytes) throws IOException {
        try (FileChannel c = FileChannel.open(file, StandardOpenOption.WRITE)) {
            c.truncate(c.size() - bytes);
        }
    }

    private static void appendBytes(Path file, byte[] bytes) throws IOException {
        Files.write(file, bytes, StandardOpenOption.APPEND);
    }

    private static void flipByte(Path file, long position) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        bytes[(int) position] ^= (byte) 0xFF;
        Files.write(file, bytes);
    }
}
