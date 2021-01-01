package com.mj.distributed.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UtilsTest {

    @Test
    void majority() {

        assertEquals(1,Utils.majority(1));
        assertEquals(2,Utils.majority(2));
        assertEquals(2,Utils.majority(3));
        assertEquals(3,Utils.majority(4));
        assertEquals(3,Utils.majority(5));
        assertEquals(4,Utils.majority(6));
        assertEquals(4,Utils.majority(7));
        assertEquals(5,Utils.majority(8));
        assertEquals(5,Utils.majority(9));
        assertEquals(6,Utils.majority(10));
        assertEquals(6,Utils.majority(11));

    }
}