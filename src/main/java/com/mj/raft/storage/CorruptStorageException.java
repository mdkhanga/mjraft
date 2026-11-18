package com.mj.raft.storage;

import java.io.IOException;

/**
 * Stored data is damaged in a way a crash cannot explain, so it cannot be trusted or repaired.
 */
public class CorruptStorageException extends IOException {

    public CorruptStorageException(String message) {
        super(message);
    }
}
