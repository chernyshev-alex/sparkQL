package com.acme.chern

import java.sql.Date
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.col
import scala.concurrent.duration._
import org.scalatest._

class ByExampleQLSpec extends FlatSpec with Matchers with LocalSparkSession  {
  
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

   lazy val sampleData = spark.sparkContext.parallelize(
    Seq(
      Tuple5("books", "Scala", "userId 100", "2018-03-01 12:00:02", "view"),
      Tuple5("books", "Scala", "userId 100", "2018-03-01 12:05:03", "view"),
      Tuple5("phones", "iPhone", "userId 300", "2018-03-01 12:10:02", "view"),
      Tuple5("phones", "iPhone", "userId 300", "2018-03-01 12:10:10", "view"),
      Tuple5("notebooks", "Macbook", "userId 100", "2018-03-01 12:00:02", "view"),
      Tuple5("notebooks", "Macbook", "userId 200", "2018-03-01 12:02:02", "view")))
    
  lazy val sampleDF = sampleData.toDF("category", "product", "userId", "eventTime", "eventType")
                .withColumn(COL_EVENT_TIME, col(COL_EVENT_TIME).cast("timestamp"))
                
  lazy val sampleDFSessions = SparkQL.genSessions(sampleDF) 
  
   it should "generate sessions Ids by (category, 5 mins intervals inactivity)" in {
    
      testSessionIdSpecification(sampleDFSessions, 5 minutes)
  }
  
  it should "generate sessions Ids by (category, 5 mins intervals inactivity) usign aggregator" in {
    
     val view = SparkQL.genSessionsWithAggregator(sampleDFSessions)
     testSessionIdSpecification(view, 5 minutes)
  }
  
  it should "SQL. find median session duration for each category" in {

    val view = SparkQL.medianSessionByCategorySQL(sampleDFSessions)

    val (books, notebooks, phones) = (view.collect()(0), view.collect()(1), view.collect()(2))

    books.getValuesMap(Seq(COL_CATEGORY, "mean")) shouldBe Map(COL_CATEGORY -> "books", "mean" -> 301)
    notebooks.getValuesMap(Seq(COL_CATEGORY, "mean")) shouldBe Map(COL_CATEGORY -> "notebooks", "mean" -> 120)
    phones.getValuesMap(Seq(COL_CATEGORY, "mean")) shouldBe Map(COL_CATEGORY -> "phones", "mean" -> 8)
  }  
  
  it should "find median session duration for each category" in {

    val view = SparkQL.medianSessionByCategory(sampleDFSessions)

    val (books, notebooks, phones) = (view.collect()(0), view.collect()(1), view.collect()(2))

    books.getValuesMap(Seq(COL_CATEGORY, "mean")) shouldBe Map(COL_CATEGORY -> "books", "mean" -> 301)
    notebooks.getValuesMap(Seq(COL_CATEGORY, "mean")) shouldBe Map(COL_CATEGORY -> "notebooks", "mean" -> 120)
    phones.getValuesMap(Seq(COL_CATEGORY, "mean")) shouldBe Map(COL_CATEGORY -> "phones", "mean" -> 8)
    
  }
  
  it should """Sample For each category find # of unique users spending less than 1 min,
    1 to 5 mins and more than 5 mins""" in {
    
    val view = SparkQL.uniqueUsersCountForPeriodsSQL(sampleDFSessions)
    
    val (books, notebooks, phones) = (view.collect()(0), view.collect()(1), view.collect()(2))
    
    books.getValuesMap(Seq(COL_CATEGORY, "LESS1M", "BETWEEN1AND5M", "MORE5M")) shouldBe 
        Map(COL_CATEGORY -> "books", "LESS1M" -> 0, "BETWEEN1AND5M" -> 0, "MORE5M" -> 1)
    notebooks.getValuesMap(Seq(COL_CATEGORY, "LESS1M", "BETWEEN1AND5M", "MORE5M")) shouldBe 
        Map(COL_CATEGORY -> "notebooks", "LESS1M" -> 0, "BETWEEN1AND5M" -> 1, "MORE5M" -> 0)
    phones.getValuesMap(Seq(COL_CATEGORY, "LESS1M", "BETWEEN1AND5M", "MORE5M")) shouldBe 
        Map(COL_CATEGORY -> "phones", "LESS1M" -> 1, "BETWEEN1AND5M" -> 0, "MORE5M" -> 0)
    
  }
  
  it should "for each category print top 10 products ranked by time spend user on product page" in {

    val view = SparkQL.topNProducts(sampleDFSessions)
   
    val (books, notebooks, phones) = (view.collect()(0), view.collect()(1), view.collect()(2))
  
    books.getValuesMap(Seq(COL_CATEGORY, COL_PRODUCT, COL_RANK)) shouldBe Map(COL_CATEGORY -> "books", COL_PRODUCT -> "Scala", COL_RANK -> 1)
    notebooks.getValuesMap(Seq(COL_CATEGORY, COL_PRODUCT, COL_RANK)) shouldBe Map(COL_CATEGORY -> "notebooks", COL_PRODUCT -> "Macbook", COL_RANK -> 1)
    phones.getValuesMap(Seq(COL_CATEGORY, COL_PRODUCT, COL_RANK)) shouldBe Map(COL_CATEGORY -> "phones", COL_PRODUCT -> "iPhone", COL_RANK -> 1)
  }  
  
 def testSessionIdSpecification(view: DataFrame, duration: FiniteDuration) = {

    val seq = view.select("eventTime", "ssId").limit(20).map(r => (r.getTimestamp(0), r.getInt(1))).collect.toList
    val seqZipped = seq zip seq.drop(1)

    // when event1 - event2 >= 5 mins then session1 <> session2
    seqZipped.find { case (t1, t2) => Math.abs((t2._1.getTime - t1._1.getTime)) / 1000 > duration.toSeconds }
      .exists { case (t1, t2) => t1._2 != t2._2 } shouldBe true

    // when event1 - event2 < 5 mins then session1 == session2
    seqZipped.find { case (t1, t2) => Math.abs((t2._1.getTime - t1._1.getTime)) / 1000 < duration.toSeconds }
      .exists { case (t1, t2) => t1._2 == t2._2 } shouldBe true

  }  
  
}