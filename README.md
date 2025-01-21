# Confluent Exercise & Project Outline
1. Develop Avro Schema based on dataset - See `NetflixUkClickEvent.asvc`
2. Develop kafka producer to loop through dataset file and produce to kafka topic. Optimize publishing.
3. Onboard to Confluent Cloud, create kafka cluster, add schmea to registry service, create flink compute pool
```aiignore
confluent schema-registry schema create --subject netflix-uk-click-event --schema ./Documents/alex-workspace/confluent-tmm/netflix-uk-insights/src/main/resources/schemas/NetflixUkClickEvent.avsc --type avro
```
4. Run various flink statements and observe behavior and verify results (statements/results below)

# Dataset - Netflix UK Click Events
Source: kaggle https://www.kaggle.com/api/v1/datasets/download/vodclickstream/netflix-audience-behaviour-uk-movies
Mac terminal file line count: 671,737 lines
```
wc -l datasets/netflix-uk/vodclickstream_uk_movies_03.csv = 671737 datasets/netflix-uk/vodclickstream_uk_movies_03.csv
```
CC Topic Messages 'Total messages' metric: 671,736
Summary: accurate count of messages in topic (minus header row)

# Flink Sql
## Exercise 1
Calculate the average watch duration for each movie title across all users
```
SELECT `movie_id`, `movie_title`, 
    AVG(`watch_duration`) AS avg_watch_duration_seconds, COUNT(*) AS num_click_events
FROM `confluent-netflix-clickstream` 
GROUP BY `movie_id`, `movie_title` 
```

Double-click into an example to verify data & query result quality
Below we see all 15 individual events
```aiignore
select * from `confluent-netflix-clickstream` where movie_id = '1040e1c42e';
```
Results
```aiignore
║key  row_id event_time    watch_duration movie_title                              movie_genre                                         release_date movie_id   user_id    event_time_ltz                                  ║
║NULL 311928 1515342232000 1596.0         Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 5e2a4fca18 2018-01-07 11:23:52.000                         ║
║NULL 312114 1515343918000 46.0           Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 5e2a4fca18 2018-01-07 11:51:58.000                         ║
║NULL 312397 1515337158000 39.0           Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 5e2a4fca18 2018-01-07 09:59:18.000                         ║
║NULL 312582 1515337268000 731.0          Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 5e2a4fca18 2018-01-07 10:01:08.000                         ║
║NULL 312652 1515351760000 484.0          Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 5e2a4fca18 2018-01-07 14:02:40.000                         ║
║NULL 315670 1515769184000 109994.0       Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 5e2a4fca18 2018-01-12 09:59:44.000                         ║
║NULL 325763 1516967535000 858.0          Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 5e2a4fca18 2018-01-26 06:52:15.000                         ║
║NULL 344725 1518769380000 4770.0         Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e a8f8f977eb 2018-02-16 03:23:00.000                         ║
║NULL 329005 1517334285000 29.0           Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 5e2a4fca18 2018-01-30 12:44:45.000                         ║
║NULL 510877 1538805395000 5073.0         Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 36e8630a62 2018-10-06 00:56:35.000                         ║
║NULL 513858 1539127586000 3003.0         Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 59f22a5c3f 2018-10-09 18:26:26.000                         ║
║NULL 533800 1541856681000 9126.0         Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 26b8ed1f79 2018-11-10 08:31:21.000                         ║
║NULL 570988 1545703524000 1691.0         Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 9ccb527e91 2018-12-24 21:05:24.000                         ║
║NULL 589333 1546976404000 4200.0         Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 0526647a5e 2019-01-08 14:40:04.000                         ║
║NULL 658268 1553847273000 0.0            Alvin and the Chipmunks Meet the Wolfman Animation, Comedy, Family, Fantasy, Horror, Musical 2000-08-29   1040e1c42e 39df57b0bb 2019-03-29 03:14:33.000
```

Average Watch Time in Seconds 
```aiignore
SELECT `movie_id`, `movie_title`, 
    AVG(`watch_duration`) AS avg_watch_duration_seconds, COUNT(*) AS num_click_events
FROM `confluent-netflix-clickstream`
WHERE `movie_id` = '1040e1c42e' 
GROUP BY `movie_id`, `movie_title` 
```
Results
```aiignore
movie_id   movie_title                              avg_watch_duration_seconds num_click_events                                                                                      ║
║1040e1c42e Alvin and the Chipmunks Meet the Wolfman 9442.666666666666          15 
```

## Exercise 2
Analyze daily engagement patterns for each movie title. Calculate daily view counts and total watch time for each \
title to track how user interest fluctuates day by day.

Add column in prep for windowing
```
ALTER TABLE `confluent-netflix-clickstream` ADD event_time_ltz AS TO_TIMESTAMP_LTZ(event_time, 3);
```

Define click event time column as the time attribute for windowing
```
ALTER TABLE `confluent-netflix-clickstream` MODIFY WATERMARK FOR event_time_ltz AS event_time_ltz;
```

Query with a tumble window (non overlapping) by day, on a particular movie.
```aiignore
SELECT window_start, movie_id, movie_title, COUNT(*) as daily_view_count, SUM(watch_duration) as watch_time_seconds
   FROM TABLE(TUMBLE(TABLE `confluent-netflix-clickstream`, DESCRIPTOR(event_time_ltz), INTERVAL '1' DAY))
   WHERE `movie_id` = '1040e1c42e'
   GROUP BY window_start, movie_id, movie_title;
```

Results for single movie
```aiignore
window_start            movie_id   movie_title                              daily_view_count watch_time_seconds                                                                        ║
║2018-01-07 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 5                2896.0                                                                                    ║
║2018-01-12 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                109994.0                                                                                  ║
║2018-01-26 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                858.0                                                                                     ║
║2018-01-30 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                29.0                                                                                      ║
║2018-02-16 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                4770.0                                                                                    ║
║2018-10-06 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                5073.0                                                                                    ║
║2018-10-09 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                3003.0                                                                                    ║
║2018-11-10 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                9126.0                                                                                    ║
║2018-12-24 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                1691.0                                                                                    ║
║2019-01-08 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                4200.0                                                                                    ║
║2019-03-29 00:00:00.000 1040e1c42e Alvin and the Chipmunks Meet the Wolfman 1                0.0       
```
Below returns view of top performing (distinct user view count) movie titles by day.
```aiignore
SELECT  *
FROM (
   SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY distinct_daily_user_views DESC ) as daily_popularity_rank
      FROM (
            SELECT window_start, window_end, movie_id, movie_title, COUNT(*) as daily_click_count, COUNT(distinct user_id) as distinct_daily_user_views, SUM(watch_duration) as watch_time_seconds
               FROM TABLE(TUMBLE(TABLE `confluent-netflix-clickstream`, DESCRIPTOR(event_time_ltz), INTERVAL '1' DAY))
            GROUP BY window_start, window_end, movie_id, movie_title
              )
) WHERE daily_popularity_rank =1 ;
```
Results
```aiignore
window_start            window_end              movie_id   movie_title                                     daily_click_count distinct_daily_user_views watch_time_seconds daily_popularity_rank                        ║
║2016-12-31 00:00:00.000 2017-01-01 00:00:00.000 8762763a6b Zookeeper                                       3                 2                         95121.0            1                                            ║
║2017-01-01 00:00:00.000 2017-01-02 00:00:00.000 f77e500e7a London Has Fallen                               25                20                        736847.0           1                                            ║
║2017-01-02 00:00:00.000 2017-01-03 00:00:00.000 f77e500e7a London Has Fallen                               23                19                        1210825.0          1                                            ║
║2017-01-03 00:00:00.000 2017-01-04 00:00:00.000 f77e500e7a London Has Fallen                               18                15                        584389.0           1                                            ║
║2017-01-04 00:00:00.000 2017-01-05 00:00:00.000 f77e500e7a London Has Fallen                               18                17                        1692027.0          1                                            ║
║2017-01-05 00:00:00.000 2017-01-06 00:00:00.000 f77e500e7a London Has Fallen                               21                17                        840336.0           1                                            ║
║2017-01-06 00:00:00.000 2017-01-07 00:00:00.000 f77e500e7a London Has Fallen                               11                10                        211809.0           1                                            ║
║2017-01-07 00:00:00.000 2017-01-08 00:00:00.000 f77e500e7a London Has Fallen                               18                13                        202614.0           1                                            ║
║2017-01-08 00:00:00.000 2017-01-09 00:00:00.000 57e2731b38 Coin Heist                                      13                9                         36505.0            1                      
```


# Observations / Questions
* Created schema via cli but UI/console only allowed me to create a new data contract, rather than associate an \
existing schema (within registry) to an already created topic. So, I deleted both again to do all in one via UI/console.\
Documentation references "Schema tab, within topics section".
* Documentation reflects 'Scheam tab' but has changed to 'Data contracts'
* Tried changing table to batch mode but received unsupported error 
* Speed Bump: trying to run with the java client/producer timing out waitOnMetadata()\
java 23? -Djava.security.manager=allow https://github.com/microsoft/mssql-jdbc/issues/2524
* watermarks - time col in long vs string date format : https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing.html
* Data Quality: Movie Release Date field can have a value of 'NOT AVAILABLE'. Therefore, Schema will treat this field value as a String
* 