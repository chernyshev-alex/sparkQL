
@see 

test/**/RequirementsSpec.scala  - test
src/**/SparkQL.scala  - implemetation


type :  /> sbt test

You should see 

=== Test 0. show session Ids
+-------------+--------------------+--------+-------------------+-----------------+------+----------+
|     category|             product|  userId|          eventTime|        eventType|ss_len| sessionId|
+-------------+--------------------+--------+-------------------+-----------------+------+----------+
|    notebooks|      MacBook Pro 15|user 100|2018-03-01 12:15:08| view description|    62| 438510572|
|        books|   Scala for Dummies|user 100|2018-03-01 12:00:02| view description|    98|-673216556|
|        books|   Scala for Dummies|user 100|2018-03-01 12:01:40|             like|    10|-673216556|
|        books|   Scala for Dummies|user 100|2018-03-01 12:01:50|     check status|    24|-673216556|
|        books|    Java for Dummies|user 100|2018-03-01 12:02:14| view description|    31|-673216556|
|        books|    Java for Dummies|user 100|2018-03-01 12:02:45|          dislike|     5|-673216556|
|        books|    Java for Dummies|user 100|2018-03-01 12:02:50|close description|   100|-673216556|
|        books|   Scala for Dummies|user 100|2018-03-01 12:04:30| view description|   105|-673216556|
|mobile phones|       iPhone 8 Plus|user 100|2018-03-01 12:06:49| view description|    25|  83897342|
|mobile phones|       iPhone 8 Plus|user 100|2018-03-01 12:07:14|    add to bucket|    47|  83897342|
|mobile phones|            iPhone 8|user 100|2018-03-01 12:08:01| view description|   309|  83897342|
|    notebooks|      MacBook Pro 13|user 200|2018-03-01 12:15:33| view description|   112| 336532731|
|    notebooks|      MacBook Pro 13|user 200|2018-03-01 12:17:25|             like|    31| 336532731|
|mobile phones|            iPhone X|user 300|2018-03-01 12:05:03| view description|    17| 706506664|
|    notebooks|         MacBook Air|user 300|2018-03-01 12:14:58| view description|   312|1884958192|
|        books|Sherlock Holmes, ...|user 200|2018-03-01 12:11:25| view description|    50| 357067878|
|        books|Sherlock Holmes, ...|user 200|2018-03-01 12:12:15|     check status|    60| 357067878|
|        books|Sherlock Holmes, ...|user 200|2018-03-01 12:13:15| sign for updates|   115| 357067878|
+-------------+--------------------+--------+-------------------+-----------------+------+----------+

=== Test 1. median session duration for each category === 
+-------------+-----+
|     category| mean|
+-------------+-----+
|        books|299.0|
|    notebooks|172.0|
|mobile phones|199.0|
+-------------+-----+

(=== Test 2. median session duration for each category === 
,Map(less1m -> 1, in1-5m -> 3, more5m -> 3))


===Test 3 Top 10 products ranked by time spend user on product page
+-------------+--------------------+---+----+
|     category|             product|len|rank|
+-------------+--------------------+---+----+
|        books|   Scala for Dummies|237|   1|
|        books|Sherlock Holmes, ...|225|   2|
|        books|    Java for Dummies|136|   3|
|    notebooks|         MacBook Air|312|   1|
|    notebooks|      MacBook Pro 13|143|   2|
|    notebooks|      MacBook Pro 15| 62|   3|
|mobile phones|            iPhone 8|309|   1|
|mobile phones|       iPhone 8 Plus| 72|   2|
|mobile phones|            iPhone X| 17|   3|
+-------------+--------------------+---+----+

[info] RequirementsSpec:
[info] - should follow session specification and generate sessionId correctly
[info] - should find median session duration for each category
[info] - should For each category find # of unique users spending less than 1 min,
[info]     1 to 5 mins and more than 5 mins
[info] - should for each category print top 10 products ranked by time spend user on product page
[info] Run completed in 24 seconds, 663 milliseconds.
[info] Total number of tests run: 4
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 4, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.


