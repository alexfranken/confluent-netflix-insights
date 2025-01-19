package com.alexfranken.confluent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(DateUtils.class);
    private static final Map<String, DateTimeFormatter> cache = new HashMap<>();

    private static DateTimeFormatter getDateTimeFormatter(String sourceFormat){
        DateTimeFormatter dateTimeFormatter;
        if(cache.containsKey(sourceFormat)){
            dateTimeFormatter = cache.get(sourceFormat);
        }else{
            LOGGER.info("DateTimeFormatter cache entry for source format {}", sourceFormat);
            dateTimeFormatter = DateTimeFormatter.ofPattern(sourceFormat);
            cache.put(sourceFormat, dateTimeFormatter);
        }
        return dateTimeFormatter;
    }

    public static long toUtcTimestamp(String part, String sourceFormat, ZoneId zoneId){
        LocalDateTime ldt = LocalDateTime.parse(part , getDateTimeFormatter(sourceFormat));
        ZonedDateTime zdt = ldt.atZone(zoneId);
        long utcTimestamp = zdt.toInstant().toEpochMilli();
//        LOGGER.debug("source:{} vs long rep {} vs deserialized: {}",
//                part, utcTimestamp, utcToZonedDateString(utcTimestamp,zoneId, "yyyy-MM-dd HH:mm:ss,z"));
        return utcTimestamp;
    }

    public static String utcToZonedDateString(long utcTimestamp, ZoneId zoneId, String destinationFormat) {
        Instant instant = new Date(utcTimestamp).toInstant();
        ZonedDateTime zdt = instant.atZone(zoneId);
        return zdt.format(getDateTimeFormatter(destinationFormat));
    }
}
