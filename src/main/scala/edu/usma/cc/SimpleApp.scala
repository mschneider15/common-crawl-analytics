package edu.usma.cc

import java.io._
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

object SimpleApp {
  def main(args: Array[String]) {
    val firstDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084887660.30/wet/*"
    val secondDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084891105.83/wet/*"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(firstDir, secondDir)
    
    val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    val emails = logData.rdd.flatMap{line => emailPattern.findAllMatchIn(line)}
    
    emails.saveAsTextFile("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")
    spark.stop()
  }
}
