package com.acme.chern

import java.sql.Date
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.col
import org.scalatest._

class RequirementsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  lazy val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
  
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val sample = spark.sparkContext.parallelize(Seq(
    Tuple5("books", "Scala", "userId 100", "2018-03-01 12:00:02", "view"),
    Tuple5("books", "Scala", "userId 100", "2018-03-01 12:05:03", "view"),
    Tuple5("phones", "Scala", "userId 300", "2018-03-01 12:10:02", "view"),
    Tuple5("phones", "Scala", "userId 300", "2018-03-01 12:10:02", "view"),
    Tuple5("phones", "Scala", "userId 300", "2018-03-01 12:10:02", "view"),
    Tuple5("phones", "Scala", "userId 300", "2018-03-01 12:20:02", "view"),
    Tuple5("phones", "Scala", "userId 300", "2018-03-01 12:21:02", "view"),
    Tuple5("phones", "Scala", "userId 300", "2018-03-01 12:22:02", "view"),
    Tuple5("phones", "Scala", "userId 300", "2018-03-01 12:23:02", "view"),
    Tuple5("phones", "Scala", "userId 300", "2018-03-01 12:30:02", "view"),
    Tuple5("notebooks", "Scala", "userId 100", "2018-03-01 12:00:02", "view"),
    Tuple5("notebooks", "Scala", "userId 200", "2018-03-01 12:02:02", "view")))

  lazy val sampleDF = sample.toDF("category", "product", "userId", "eventTime", "eventType")
    .withColumn(COL_EVENT_TIME, col(COL_EVENT_TIME).cast("timestamp"))

  val PathToCsvFile = "./data/input.csv"

  lazy val csvDataFrame = SparkQL.loadCsvDataFrame(PathToCsvFile)(spark)
  lazy val dfSessions = SparkQL.genSessions(csvDataFrame)(spark)

  // Session Id specification
  
  it should "follow session specification and generate sessionId correctly" in {
    
    val result = SparkQL.genSessions(sampleDF)(spark).collect()

    val phonesUser300_1210 = result(0)
    val phonesUser300_1220 = result(1)
    val phonesUser300_1221 = result(2)
    val booksUser100_1200  = result(5)

    // should have different sessionIds, because time elapsed > 5 mins
    phonesUser300_1210.getAs[BigInt](COL_SESSION_ID) ne phonesUser300_1220.getAs[BigInt](COL_SESSION_ID)
    // lasts 10 minutes
    phonesUser300_1210.getAs[BigInt]("ss_len") shouldBe 600
    //  should have the same sessionId because only 1 minutes passed between clicks for the (category, user)
    phonesUser300_1210.getAs[BigInt](COL_SESSION_ID) eq phonesUser300_1221.getAs[BigInt](COL_SESSION_ID)
  }

  // SQL queries specification
  
  it should "show session Ids" in {
     println("=== Test 0. show session Ids")
     dfSessions.show
  }
  
  it should "find median session duration for each category" in {
    
    val df = SparkQL.medianSessionByCategory(dfSessions)(spark)
    
    println("=== Test 1. median session duration for each category === ")   
    df.show
    
    val result = df.collect()
    val booksMean = result(0)
    
    booksMean.getAs[BigInt](COL_CATEGORY) shouldBe "books" 
    booksMean.getAs[Float]("mean") shouldBe 299.0
  } 

  it should """For each category find # of unique users spending less than 1 min,
    1 to 5 mins and more than 5 mins""" in {
    
    val result = SparkQL.uniqueUsersCountForPeriods(dfSessions)(spark)
    println("=== Test 2. For each category find # of unique users spending < 1m, 1-5m, >5m === \n", result)
    result shouldBe Map("less1m" -> 1, "in1-5m" -> 3, "more5m" -> 3)
  }

  it should "for each category print top 10 products ranked by time spend user on product page" in {
    
    val df = SparkQL.top10Products(dfSessions)(spark)
    println("\n===Test 3 Top 10 products ranked by time spend user on product page")
    df.show
    
    val result = df.collect()
    val booksMean = result(0)
    
    booksMean.getAs[String](COL_CATEGORY) shouldBe "books" 
    booksMean.getAs[Int](COL_LEN) shouldBe 237
    booksMean.getAs[Int](COL_RANK) shouldBe 1
  }    

}