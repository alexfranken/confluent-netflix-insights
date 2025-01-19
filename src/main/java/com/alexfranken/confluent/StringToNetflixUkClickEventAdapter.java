package com.alexfranken.confluent;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;

/**
 *
 */
public class StringToNetflixUkClickEventAdapter {
    public static final String SCHEMA_PATH = "schemas/NetflixUkClickEvent.avsc";
    public static final String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final ZoneId UK_ZONE_ID = ZoneId.of("Europe/London");
    private final Schema targetSchema;
    public StringToNetflixUkClickEventAdapter() throws IOException {

        try (
                InputStream s = this.getClass().getClassLoader()
                        .getResourceAsStream(SCHEMA_PATH)) {
            targetSchema = new Schema.Parser().parse(s);
        }
    }

    public GenericRecord adapt(String line){
        String[] parts = line.split(lineRegex(), -1);
        GenericRecord record = new GenericData.Record(this.targetSchema);
        record.put("row_id", Integer.parseInt(parts[0]));
        record.put("event_time", DateUtils.toUtcTimestamp(parts[1], SOURCE_FORMAT, UK_ZONE_ID));
        record.put("watch_duration", Double.parseDouble(parts[2]));
        record.put("movie_title", sanitizeString(parts[3]));
        record.put("movie_genre", sanitizeString(parts[4]));
        record.put("release_date", parts[5]);//DateUtils.toLocalDate(parts[5]));
        record.put("movie_id", parts[6]);
        record.put("user_id",parts[7]);
        return record;
    }

    private String lineRegex() {
        //regex help: https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ // enable comments, ignore white spaces
                        ",                         "+ // match a comma
                        "(?=                       "+ // start positive look ahead
                        "  (?:                     "+ //   start non-capturing group 1
                        "    %s*                   "+ //     match 'otherThanQuote' zero or more times
                        "    %s                    "+ //     match 'quotedString'
                        "  )*                      "+ //   end group 1 and repeat it zero or more times
                        "  %s*                     "+ //   match 'otherThanQuote'
                        "  $                       "+ // match the end of the string
                        ")                         ", // stop positive look ahead
                otherThanQuote, quotedString, otherThanQuote);
        return regex;
    }

    private String sanitizeString(String part) {
        StringBuilder sb = new StringBuilder();
        if(part != null && part.startsWith("\"") && part.endsWith("\"")){
            return part.substring(1,part.length()-1);
        }
        return part;
    }
}
