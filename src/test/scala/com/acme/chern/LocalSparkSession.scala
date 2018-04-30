package com.acme.chern

import org.apache.spark.sql.SparkSession

trait LocalSparkSession {
  
    implicit lazy val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()

}