package org.formation;

import com.google.common.annotations.VisibleForTesting;
import org.formation.model.Position;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class PositionTest {

    @Test
    void testEquals() {
        Position p1 = new Position(0.0, 0.0);
        Position p2 = new Position(0.0, 0.0);
        Position p3 = new Position(1.0, 0.0);

        assertEquals(p1,p2);
        assertNotEquals(p1,p3);
    }
}
