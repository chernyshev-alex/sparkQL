package com.acme.chern

import java.sql.Date
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.col
import scala.concurrent.duration._
import org.scalatest._

class SparkQLSpec extends FlatSpec with Matchers with LocalSparkSession  {

  import spark.implicits._
  
  spark.sparkContext.setLogLevel("ERROR")

  val PathToCsvFile = "./data/input.csv"

  lazy val csvDataFrame = SparkQL.loadCsvDataFrame(PathToCsvFile)
  lazy val dfSessions = SparkQL.genSessions(csvDataFrame)

  it should "generate sessions Ids by (category, 5 mins intervals inactivity)" in {
    println("=== Test 1. generate sessions Ids by (category, 5 mins intervals inactivity)")

    dfSessions.show(50)
    // no asserts, tested with ByExampleQLSpec
  }

  it should "generate sessions Ids by (category, 5 mins intervals inactivity) usign aggregator" in {
     println("=== Test 2. generate sessions Ids usign aggregator")

     val view = SparkQL.genSessionsWithAggregator(csvDataFrame)
     view.show(50)
     
     // no asserts, tested with ByExampleQLSpec
  }

   it should "SQL. find median session duration for each category" in {

    val view = SparkQL.medianSessionByCategorySQL(dfSessions)

    println("=== Test 3.SQL find median session duration for each category === ")
    view.show(50)
    // no asserts, tested with ByExampleQLSpec
  }

  it should "find median session duration for each category" in {

    val view = SparkQL.medianSessionByCategory(dfSessions)

    println("=== Test 3. find median session duration for each category === ")
    view.show(50)
    // no asserts, tested with ByExampleQLSpec
  }

  it should """SQL For each category find # of unique users spending less than 1 min,
    1 to 5 mins and more than 5 mins""" in {

    val view = SparkQL.uniqueUsersCountForPeriodsSQL(dfSessions)
    println("=== Test 4. SQL For each category find # of unique users spending < 1m, 1-5m, >5m === \n", view)

    view.show(50)
    // no asserts, tested with ByExampleQLSpec   
  }

  it should "for each category print top 10 products ranked by time spend user on product page" in {

    val view = SparkQL.topNProducts(dfSessions)
    println("\n===Test 5. Top 10 products ranked by time spend user on product page")
    
    view.show
    // no asserts, tested with ByExampleQLSpec  
  }

}