package edu.usma.cc

import java.io._
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/p/work2/michella/crawl-data/CC-MAIN-2018-05/segments/1516084890394.46/wet/CC-MAIN-20180121080507-20180121100507-00398.warc.wet"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    val emailPattern = new Regex("""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])""")

    val result = s"Lines with a: $numAs, Lines with b: $numBs"
    new PrintWriter(new File("/p/work2/michella/crawl-results/results.txt")) { write(result); close }

    var emailResult = ""
    for (line <- logData; m <- emailPattern.findAllIn(line)) {
      emailResult += s"found email:\n  $m \n"
    }

    new PrintWriter(new File("/p/work2/michella/crawl-results/emailResults.txt")) { write(emailResult); close }
    
    spark.stop()
  }
}
