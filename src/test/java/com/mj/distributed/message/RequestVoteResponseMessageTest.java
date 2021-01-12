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
                "192.133.4.1",
                 1234,
                true
        );

        ByteBuffer b = response.serialize();

        RequestVoteResponseMessage readResponse = RequestVoteResponseMessage.deserialize(b);

        assertTrue(3 == readResponse.getTerm());
        assertTrue(1234 == readResponse.getCandidatePort());
        assertTrue("192.133.4.1".equals(readResponse.getCandidateHost()));
        assertTrue(readResponse.getVote());

    }

    public void serializeFalse() throws Exception {

        RequestVoteResponseMessage response = new RequestVoteResponseMessage(
                3,
                "192.168.33.9",
                5551,
                false
        );

        ByteBuffer b = response.serialize();

        RequestVoteResponseMessage readResponse = RequestVoteResponseMessage.deserialize(b);

        assertTrue(3 == readResponse.getTerm());
        assertFalse(readResponse.getVote());

    }
}