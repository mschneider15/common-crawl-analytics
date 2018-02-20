package edu.usma.cc

import java.io._
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084891105.83/wet/CC-MAIN-20180122054202-20180122074202-00449.warc.wet"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile)

    val emailPattern = new Regex("""[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}""")
//  val emailPattern = new Regex("""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])""")

//    for (line <- logData) {
//      val m = emailPattern.findFirstMatchIn(line).getOrElse("none")
//      if (m != "none") println(m)
//    }

    val emails = logData.flatMap(line => emailPattern.findFirstMatchIn(line))
    emails.write.format("text").save("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")
//    emails.collect()
//    emails.saveAsTextFile("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")
    spark.stop()
  }
}
