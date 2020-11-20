package com.snapscore.pipeline.utils;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class DateUtils {

    public static long toTimestamp(LocalDateTime dateTime) {
        long timestamp = dateTime.toEpochSecond(ZoneOffset.UTC) * 1000;
        return timestamp;
    }

    public static long toTimestamp(LocalDate date) {
        long timestamp = date.toEpochSecond(LocalTime.MAX, ZoneOffset.UTC) * 1000;
        return timestamp;
    }

    /**
     * @return date range including the specified from and to dates
     */
    public static List<LocalDate> createDateRange(LocalDate fromDate, LocalDate toDate) {
        int daysBetween = calcDaysBetween(fromDate, toDate);
        List<LocalDate> dates = new ArrayList<>();
        for (int dayIncr = 0; dayIncr <= daysBetween; dayIncr++) {
            dates.add(fromDate.plusDays(dayIncr));
        }
        return dates;
    }

    public static int calcDaysBetween(LocalDate start, LocalDate end) {
        return (int) ChronoUnit.DAYS.between(start, end);
    }

    public static LocalDateTime nowUTC() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    public static long nowUTCTs() {
        return System.currentTimeMillis();
    }

    public static LocalDate todayUTC() {
        return LocalDate.now(ZoneOffset.UTC);
    }

    public static LocalDateTime fromUTCTs(long timestamp) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
        return localDateTime;
    }

}
