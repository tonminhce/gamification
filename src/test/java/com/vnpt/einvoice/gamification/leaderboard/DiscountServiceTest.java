package com.vnpt.einvoice.gamification.leaderboard;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DiscountServiceTest {

    private final DiscountService service = new DiscountService();

    @ParameterizedTest
    @CsvSource({
            "0, 0",
            "99, 0",
            "100, 5",
            "499, 5",
            "500, 10",
            "999, 10",
            "1000, 15",
            "10000, 15"
    })
    void appliesDiscountAtTierBoundaries(long points, int expectedPercent) {
        assertEquals(expectedPercent, service.discountPercent(points));
    }
}
