package com.github.dhoard.util;

import java.time.Instant;
import java.time.ZoneId;

public class TimestampUtil {

    public static String getISOTimestamp() {
        return toISOTimestamp(System.currentTimeMillis(), "America/New_York");
    }

    public static String toISOTimestamp(long milliseconds, String timeZoneId) {
        return Instant
            .ofEpochMilli(milliseconds)
            .atZone(ZoneId.of(timeZoneId))
            .toString()
            .replace("[" + timeZoneId + "]", "");
    }
}
