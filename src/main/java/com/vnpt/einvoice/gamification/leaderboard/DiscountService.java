package com.vnpt.einvoice.gamification.leaderboard;

import org.springframework.stereotype.Service;

@Service
public class DiscountService {

    public int discountPercent(long points) {
        if (points >= 1_000) {
            return 15;
        }
        if (points >= 500) {
            return 10;
        }
        if (points >= 100) {
            return 5;
        }
        return 0;
    }
}
