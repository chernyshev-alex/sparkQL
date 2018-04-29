
@see 

test/**/RequirementsSpec.scala  - test
src/**/SparkQL.scala  - implementation
src/**/SessionGenerator.scala  - aggregator impl

type :  /> sbt test

generated  views and all test are passed

[info] RequirementsSpec:
[info] - should generate sessions Ids by (category, 5 mins intervals inactivity)
[info] - should generate sessions Ids by (category, 5 mins intervals inactivity) usign aggregator
[info] - should SQL. find median session duration for each category
[info] - should find median session duration for each category
[info] - should SQL For each category find # of unique users spending less than 1 min,
[info]     1 to 5 mins and more than 5 mins
[info] - should for each category print top 10 products ranked by time spend user on product page
[info] Run completed in 18 seconds, 736 milliseconds.
[info] Total number of tests run: 6
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 6, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.


