package com.alexfranken.confluent;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateUtils {
    private static final Map<String, DateTimeFormatter> cache = new HashMap<>();

    private static DateTimeFormatter getDateTimeFormatter(String sourceFormat){
        DateTimeFormatter dateTimeFormatter;
        if(cache.containsKey(sourceFormat)){
            dateTimeFormatter = cache.get(sourceFormat);
        }else{
            dateTimeFormatter = DateTimeFormatter.ofPattern(sourceFormat);
            cache.put(sourceFormat, dateTimeFormatter);
        }
        return dateTimeFormatter;
    }

    public static long toUtcTimestamp(String part, String sourceFormat, ZoneId zoneId){
        LocalDateTime ldt = LocalDateTime.parse(part , getDateTimeFormatter(sourceFormat));
        ZonedDateTime zdt = ldt.atZone(zoneId);
        long utcTimestamp = zdt.toInstant().toEpochMilli();
        System.out.printf("source:%s vs long rep %d vs deserialized: %s%n", part, utcTimestamp, utcToZonedDateString(utcTimestamp,zoneId, "yyyy-MM-dd HH:mm:ss,z"));
        return utcTimestamp;
/*
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // UK timezone
        ZoneId ukZoneId = ZoneId.of("Europe/London");
        formatter.withZone(ukZoneId);
        long epochTime = Timestamp.valueOf(LocalDateTime.parse(part, formatter)).getTime();
        System.out.printf("source:%s vs long rep %d vs deserialized: %s", part, epochTime, new Date(epochTime));//TODO

 */
    }

    public static String utcToZonedDateString(long utcTimestamp, ZoneId zoneId, String destinationFormat) {
        Instant instant = new Date(utcTimestamp).toInstant();
        ZonedDateTime zdt = instant.atZone(zoneId);
        return zdt.format(getDateTimeFormatter(destinationFormat));
    }

    public static long toLocalDate(String part){
        LocalDate serLd = LocalDate.parse(part);
        long daysSinceEpoch = serLd.toEpochDay();
        /* //deserialize
        LocalDate desLd = LocalDate.ofEpochDay(daysSinceEpoch);
        System.out.printf("Source %s Serialized %d Deserialized %s", part, daysSinceEpoch, desLd.toString());
         */
        return daysSinceEpoch;
    }
}
