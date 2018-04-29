package com.acme.chern

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ DataFrame, SparkSession }

object SparkQL {
  
  def main(args: Array[String]) : Unit = {
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
   * Generate a new session on each (category, event2 - event1 > 5 minutes) 
   */
  def genSessions(df: DataFrame, inactivityInterval : Int = 5 * 60)(implicit ss: SparkSession): DataFrame = {
    
    val windowCategory = Window.partitionBy(col(COL_CATEGORY)).orderBy(col(COL_EVENT_TIME))
    val windowSession = (coalesce(unix_timestamp(col(COL_EVENT_TIME)) - unix_timestamp(lag(col(COL_EVENT_TIME), 1)
                            .over(windowCategory)), lit(0)) > inactivityInterval)
                        .cast("bigint")
    
    val SS_COLUMN_TMP = "_ss"
 
    val sessionsOverCategory = df.withColumn(SS_COLUMN_TMP, sum(windowSession).over(windowCategory))
    
    // Generate ss column - session markup within a category
    //
    // 'category' 'eventTime' '_ss'  <other cols ..>
    // category_1  10:55:00     0
    // category_1  11:00:01     1   # when 11:00:01 - 10:55:00 > 5 minutes, increment temporary column ss for given category
    // category_1  11:01:00     1
    // category_2  10:30:00     0
    ////
    val wss = Window.partitionBy(COL_CATEGORY, SS_COLUMN_TMP)
    
    val soc = sessionsOverCategory.withColumn("ssId",  hash(col(COL_CATEGORY), col(SS_COLUMN_TMP)))
                                  .withColumn("ss_start",   min(col(COL_EVENT_TIME)).over(wss))
                                  .withColumn("ss_end",     max(col(COL_EVENT_TIME)).over(wss))
                  
    // generate slen - partial session duration for each action
    // remove temporary _ss column       
    val ssLengthExpr =  unix_timestamp(lag(col(COL_EVENT_TIME), -1).over(windowCategory)) - unix_timestamp(col(COL_EVENT_TIME))                              
    soc.withColumn(COL_SS_LEN, ssLengthExpr).drop(SS_COLUMN_TMP)
  }

  /**
   * Generate a new session on each (category, event2 - event1 > 5 minutes)  using aggregator 
   */
   def genSessionsWithAggregator(df: DataFrame, inactivityInterval : Int = 5 * 60)(implicit ss: SparkSession) : DataFrame = {   

      val windowCategory = Window.partitionBy(col(COL_CATEGORY)).orderBy(col(COL_EVENT_TIME))

      val sessionGenerator = new SessionGenerator(inactivityInterval)
      val ssColumnExpr = sessionGenerator(col("category"), col("eventTime")).over(windowCategory)
      
      df.withColumn("ssId", ssColumnExpr)
        .withColumn("ss_start", min(col(COL_EVENT_TIME)).over(windowCategory))
        .withColumn("ss_end",   max(col(COL_EVENT_TIME)).over(windowCategory))
   }     
    
  /**
   * Find median session duration for each category
   */
  def medianSessionByCategory(dfWithSessions : DataFrame)(implicit ss: SparkSession) : DataFrame = {
    
    dfWithSessions.groupBy(col(COL_CATEGORY)).agg(round(avg(col(COL_SS_LEN))).as(COL_MEAN))
  }
  
  def medianSessionByCategorySQL(dfWithSessions : DataFrame)(implicit ss: SparkSession) : DataFrame = {   
    
    dfWithSessions.createOrReplaceTempView("events")    
    dfWithSessions.sqlContext.sql("select category, round(avg(ss_len)) as mean from events group by category")
  }

 def uniqueUsersCountForPeriodsSQL(dfWithSessions : DataFrame)(implicit ss: SparkSession) : DataFrame = {
   
    dfWithSessions.createOrReplaceTempView("events")    
    
    dfWithSessions.sqlContext.sql("""select category, 
        sum(case when ss_len < 60 then 1 else 0 end) as LESS1M,
        sum(case when ss_len between 60 and 300 then 1 else 0 end) as BETWEEN1AND5M,
        sum(case when ss_len > 300 then 1 else 0 end) as MORE5M
      from (
        select category, userId, sum(ss_len) as ss_len from events group by category, userId
      ) group by category""")
  }  
  
  /**
   * For each category print top 10 products ranked by time spend user on product page
   */
  def topNProducts(dfWithSessions : DataFrame, topN : Int = 10)(implicit ss: SparkSession) : DataFrame = {
    
    val view = dfWithSessions.select("*").where(col("eventType").like("view%"))
        .groupBy(col(COL_CATEGORY), col(COL_PRODUCT)).agg(sum(col(COL_SS_LEN)).as(COL_SS_LEN))

    val overCategory = Window.partitionBy(col(COL_CATEGORY)).orderBy(desc(COL_SS_LEN))
    view.withColumn(COL_RANK, dense_rank.over(overCategory)).where(col(COL_RANK) <= topN)
  }
  
}