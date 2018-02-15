package edu.usma.cc

import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084891105.83/wet/CC-MAIN-20180122054202-20180122074202-00449.warc.wet"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()

    val emailPattern = new Regex("""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])""")

    val emails = logData.filter(line => emailPattern.findFirstMatchIn(line).getOrElse("None.") != "None.")
    emails.collect()
    emails.saveAsTextFile("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")
    spark.stop()
  }
}
