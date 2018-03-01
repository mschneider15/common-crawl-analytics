package edu.usma.cc

import java.io._
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

object SimpleApp {
  def main(args: Array[String]) {
    val firstDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084887660.30/wet/*"
    val secondDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084891105.83/wet/*"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(firstDir,secondDir)
    
    // The following are a few different REGEXs which can be used in finding emails
    // emailPattern is the most simple
    // val emailPattern = new Regex("""[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}""")
    // emailPattern1 is the most complex. It is prone to a stackoverflow error due to its complexity
    // val emailPattern1 = new Regex("""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\]){6,64}""")
    // emailPattern2 is another simple REGEX
    // val emailPattern2 = new Regex("""([\\w-+]+(?:\\.[\\w-+]+)*@(?:[\\w-]+\\.)+[a-zA-Z]{2,7})""")
    // emailPattern3 is a REGEX which would, in theory, be used to filter out words greater than 64 characters which do not contain an @ symbol
    // val emailPattern3 = new Regex("""(?:[a-z0-9._%+-]+@[A-Z0-9.-]\.[A-Z]){8,64}""")
    // emailPattern4 is a REGEX which is simpler, yet ought still catch many, if not all emails
    val emailPattern4 = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    val emails = logData.rdd.flatMap{line => emailPattern4.findAllMatchIn(line)}
    //emails.write.format("text").save("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")
//    emails.collect()
    emails.saveAsTextFile("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")
    spark.stop()
  }
}
