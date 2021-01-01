package com.mj.distributed.message;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestVoteResponseMessageTest {

    @Test
    public void serializeTrue() throws Exception {

        RequestVoteResponseMessage response = new RequestVoteResponseMessage(
                3,
                4,
                true
        );

        ByteBuffer b = response.serialize();

        RequestVoteResponseMessage readResponse = RequestVoteResponseMessage.deserialize(b);

        assertTrue(3 == readResponse.getTerm());
        assertTrue(4 == readResponse.getCandidateId());
        assertTrue(readResponse.getVote());

    }

    public void serializeFalse() throws Exception {

        RequestVoteResponseMessage response = new RequestVoteResponseMessage(
                3,
                4,
                false
        );

        ByteBuffer b = response.serialize();

        RequestVoteResponseMessage readResponse = RequestVoteResponseMessage.deserialize(b);

        assertTrue(3 == readResponse.getTerm());
        assertTrue(4 == readResponse.getCandidateId());
        assertFalse(readResponse.getVote());

    }
}