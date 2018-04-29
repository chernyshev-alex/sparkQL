package com.acme.chern

import java.sql.Date
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.col
import scala.concurrent.duration._
import org.scalatest._

class RequirementsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit lazy val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
  
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val PathToCsvFile = "./data/input.csv"

  lazy val csvDataFrame = SparkQL.loadCsvDataFrame(PathToCsvFile)(spark)
  lazy val dfSessions = SparkQL.genSessions(csvDataFrame)(spark)


  it should "generate sessions Ids by (category, 5 mins intervals inactivity)" in {    
     println("=== Test 1. generate sessions Ids by (category, 5 mins intervals inactivity)")
     
     val view = dfSessions

     view.show(100)

     testSessionIdSpecification(view)     
   }
  
  it should "generate sessions Ids by (category, 5 mins intervals inactivity) usign aggregator" in {    
     println("=== Test 2. generate sessions Ids usign aggregator")
    
     val view = SparkQL.genSessionsWithAggregator(csvDataFrame)
     view.show(100)
     
     testSessionIdSpecification(view)
  }
  
   it should "SQL. find median session duration for each category" in {
    
    val view = SparkQL.medianSessionByCategorySQL(dfSessions)
    
    println("=== Test 3.SQL find median session duration for each category === ")   
    view.show
    
    val booksMean = view.collect()(0)
    
    booksMean.getAs[String](COL_CATEGORY) shouldBe "books" 
    booksMean.getAs[Float]("mean") shouldBe 70.0
  } 
  
   
  it should "find median session duration for each category" in {
    
    val view = SparkQL.medianSessionByCategory(dfSessions)
    
    println("=== Test 3. find median session duration for each category === ")   
    view.show
    
    val booksMean = view.collect()(0)
    
    booksMean.getAs[String](COL_CATEGORY) shouldBe "books" 
    booksMean.getAs[Float]("mean") shouldBe 70.0
  } 
  
  
  it should """SQL For each category find # of unique users spending less than 1 min,
    1 to 5 mins and more than 5 mins""" in {
    
    val view = SparkQL.uniqueUsersCountForPeriodsSQL(dfSessions)
    println("=== Test 4.SQL For each category find # of unique users spending < 1m, 1-5m, >5m === \n", view)
    
    view.show
    
    val books = view.collect()(0)
    books.getAs[String](COL_CATEGORY) shouldBe "books" 
    books.getAs[Int]("LESS1M") shouldBe 0
    books.getAs[Int]("BETWEEN1AND5M") shouldBe 1
    books.getAs[Int]("MORE5M") shouldBe 1
  }
  
  
  it should "for each category print top 10 products ranked by time spend user on product page" in {
    
    val view = SparkQL.topNProducts(dfSessions)
    println("\n===Test 5. Top 10 products ranked by time spend user on product page")
    view.show
    
    val booksMean = view.collect()(0)
    
    booksMean.getAs[String](COL_CATEGORY) shouldBe "books" 
    booksMean.getAs[Int](COL_SS_LEN) shouldBe 203
    booksMean.getAs[Int](COL_RANK) shouldBe 1
  }    
 
  // test utils
  
    def testSessionIdSpecification(view : DataFrame) = {
    
     val seq = view.select("eventTime", "ssId").limit(20).map(r => (r.getTimestamp(0), r.getInt(1))).collect.toList 
     val seqZipped = seq zip seq.drop(1)
     
     val duration = 5 minutes
     
     // when event1 - event2 >= 5 mins then session1 <> session2
     seqZipped.find { case (t1, t2) => (t2._1.getTime - t1._1.getTime) / 1000 > duration.toSeconds }
       .exists { case (t1, t2)  => t1._2 != t2._2 }  shouldBe true

     // when event1 - event2 < 5 mins then session1 == session2
     seqZipped.find { case (t1, t2) => (t2._1.getTime - t1._1.getTime) /1000 < duration.toSeconds }
       .exists { case (t1, t2)  => t1._2 == t2._2 } shouldBe true
   
  }
 
  
  
}