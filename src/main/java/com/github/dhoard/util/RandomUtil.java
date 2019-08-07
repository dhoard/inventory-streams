package com.github.dhoard.util;

import java.util.Random;

public class RandomUtil {

    private static final Random random = new Random();

    public static long randomLong(int min, int max) {
        if (max == min) {
            return min;
        }

        return + (long) (random.nextDouble() * (max - min));
    }
}
