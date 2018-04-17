package com.acme.chern

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ DataFrame, SparkSession }
import scala.concurrent.duration._

object SparkQL {
  
  def main(args: Array[String]): Unit = {
    println("type : sbt test")
  }

  /**
   * Load csv file and change type of "eventTime" column to timestamp
   */
  def loadCsvDataFrame(pathToFile: String)(implicit ss: SparkSession): DataFrame = {
    ss.read.format("csv").option("header", "true").load(pathToFile)
      .withColumn(COL_EVENT_TIME, col(COL_EVENT_TIME).cast("timestamp"))
  }

  /**
   * Start a new session on (category, userId, event2 - event1 > 5 minutes) 
   * Using Window and function lag to create a new columns layout
   *    |   timeEvent  |        ss_end        |          ss_len
   *    | time event1  | time event2          | time event2 - time event1
   *    | time event2  | time event3          | time event3 - time event2
   *    | time event3  | null ( > 5 mins)     | 0
   *
   *  It could be translated to :
   *  
   *  select category, userId, eventTime, product, eventType, 
   *  		ss_len = ss_end - eventTime, 
   *    	sessionId = hash(category, userId, session.end)
   *  	from (
   *  		select *, ss_end = lag(event_time, -1), session = window(event_time, 5 minutes) 
   *  			from df 
   *  			group by category, userid 
   *  			order by eventTime )
   * 
   *  where :
   *  	  sessionId 	- generated unique session identifier
   *    ss_len  		- duration of the event , next(eventTime) - current(eventTime) for (category, userId) in 5 minutes
   * 
   */
  def genSessions(df: DataFrame)(implicit ss: SparkSession): DataFrame = {

    val wCategory = Window.partitionBy(col(COL_CATEGORY), col(COL_USER_ID)).orderBy(col(COL_EVENT_TIME))

    val windowLength = 5 minutes
    val tmp = df.withColumn("ss_end", lag(COL_EVENT_TIME, -1).over(wCategory))
      .withColumn(
        COL_SESSION,
        window(col(COL_EVENT_TIME), windowLength.toString()))

    val sessionId = hash(col(COL_CATEGORY), col(COL_USER_ID), col(COL_SESSION + ".end")).cast("bigint")

    // calculate sessions duration in seconds and remove sessions with ss_len == 0 to get mean/avg() works correctly
    tmp.withColumn(COL_SS_LEN, unix_timestamp(col("ss_end")) - unix_timestamp(col(COL_EVENT_TIME)))
      .select(COL_ALL).where(col(COL_SS_LEN) > 0)
      .withColumn(COL_SESSION_ID, sessionId).drop(COL_SESSION).drop("ss_end")
  }

  /**
   * Find median session duration for each category
   * 
   * It could be translated to :
   *  
   *  select category, mean(len) as mean
   *  from (
   *  		select category, userId, len = sum(ss_len) from df 
   *  ) 
   *  group by category
   */
  def medianSessionByCategory(df: DataFrame)(implicit ss: SparkSession) : DataFrame = {
    df.groupBy(col(COL_CATEGORY), col(COL_USER_ID)).agg(sum(col(COL_SS_LEN)).as(COL_LEN))
      .groupBy(col(COL_CATEGORY)).agg(round(mean(col(COL_LEN))).as(COL_MEAN))
  }

  /**
   * For each category find # of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins
   *  
   *  It could be translated to :
   *  
   *  select sum(less1m) as less1m, sum(in1-5m) as in1-5m, sum(more5m) as more5m
   *  from (
   *  		select less1m = count(*), in1-5m = 0, more5m = 0 from tmp where len < 60
   *  		union all
   *  		select 0, count(*), 0 from tmp where len between (60, 300)
   *  		union all
   *  		select 0, 0, count(*) from tmp where len > 300
   *  )
   *  
   *  where : count(*) equals count(userId) because we have unique userId's in tmp dataframe
   *  
   */
  def uniqueUsersCountForPeriods(df: DataFrame)(implicit ss: SparkSession) : Map[String, Long] = {

    val tmp = df.groupBy(col(COL_CATEGORY), col(COL_USER_ID)).agg(sum(col(COL_SS_LEN)).as(COL_LEN))

    // TODO : should be better solution 
    
    val less1Minute = tmp.where(col(COL_LEN) <= 60).count
    val inRange1_5Minutes = tmp.where(col(COL_LEN) > 60 && col(COL_LEN) <= 300).count
    val more5Minutes = tmp.where(col(COL_LEN) > 300).count

    Map("less1m" -> less1Minute, "in1-5m" -> inRange1_5Minutes, "more5m" -> more5Minutes)
  }

  /**
   * For each category print top 10 products ranked by time spend user on product page
   * 
   *  It could be translated to :
   *  
   *  select category, product, len, rank = dense_rank over window(category order by len desc) 
   *  from (
   *  		select category, product, len = sum(ss_len) from df 
   *  ) 
   *  where runk <= 10
   */
  def top10Products(df: DataFrame)(implicit ss: SparkSession) : DataFrame = {

    val tmp = df.groupBy(col(COL_CATEGORY), col(COL_PRODUCT)).agg(sum(col(COL_SS_LEN)).as(COL_LEN))

    val overCategory = Window.partitionBy(col(COL_CATEGORY)).orderBy(desc(COL_LEN))
    tmp.withColumn(COL_RANK, dense_rank.over(overCategory)).where(col(COL_RANK) <= 10)
  }

}