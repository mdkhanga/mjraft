package com.mj.distributed.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterConfigTest {

    private static final Member A = new Member("localhost", 5001);
    private static final Member B = new Member("localhost", 5002);
    private static final Member C = new Member("localhost", 5003);

    @Test
    void majorityIsOfTheConfiguredSize() {
        assertEquals(1, ClusterConfig.single(A).majority());
        assertEquals(2, new ClusterConfig(A, List.of(A, B)).majority());
        assertEquals(2, new ClusterConfig(A, List.of(A, B, C)).majority());
        assertEquals(3, new ClusterConfig(A, List.of(A, B, C, new Member("localhost", 5004))).majority());
    }

    @Test
    void othersExcludesSelf() {
        assertEquals(List.of(A, C), new ClusterConfig(B, List.of(A, B, C)).others());
    }

    @Test
    void parseReadsHostPortList() {
        ClusterConfig c = ClusterConfig.parse(B, "localhost:5001, localhost:5002,localhost:5003");
        assertEquals(List.of(A, B, C), c.members());
    }

    @Test
    void rejectsMembersWithoutSelf() {
        assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(A, List.of(B, C)));
    }

    @Test
    void rejectsDuplicates() {
        assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(A, List.of(A, B, B)));
    }

    @Test
    void rejectsMalformedEntry() {
        assertThrows(IllegalArgumentException.class, () -> ClusterConfig.parse(A, "localhost:5001,localhost"));
    }
}
