package edu.usma.cc

import scala.io.Source
import java.io._
import java.net.URI
import scala.util.matching.Regex

import com.martinkl.warc.WARCFileReader
import com.martinkl.warc.WARCFileWriter
import com.martinkl.warc.mapreduce.WARCInputFormat
import com.martinkl.warc.mapreduce.WARCOutputFormat
import com.martinkl.warc.WARCRecord
import com.martinkl.warc.WARCRecord.Header
import com.martinkl.warc.WARCWritable

import org.apache.spark._
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io._

import org.apache.spark.sql.functions._

object SimpleApp {

  def main(args: Array[String]) {
    // Path to WARC files
    // Directory paths will allow access to all files within
    // val firstDir = "s3://commoncrawl/crawl-data/CC-MAIN-2016-18/segments/1461860106452.21/wet/CC-MAIN-20160428161506-00000-ip-10-239-7-51.ec2.internal.warc.wet.gz"
    // val warcPathFirstHalf = "s3://commoncrawl/"
    val firstDir = "file:///Users/ethan/common-crawl-analytics/warc_files/msg_0000.warc"
    val warcPathFirstHalf = "file:///Users/ethan/common-crawl-analytics/warc_files/"

    
    
    // An array of the final path segments for each directory containing warc files
    /*val warcDirs = Array(
     "crawl-data/CC-MAIN-2016-18/segments/1461860106452.21/wet/CC-MAIN-20160428161506-00001-ip-10-239-7-51.ec2.internal.warc.wet.gz", 
     "crawl-data/CC-MAIN-2016-18/segments/1461860106452.21/wet/CC-MAIN-20160428161506-00002-ip-10-239-7-51.ec2.internal.warc.wet.gz")
     */
    println("Starting cluster")

    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Open the firstDir directory as an RDD of type [(LongWritable, WARCWritable)]
    val warcInput = sc.newAPIHadoopFile(firstDir, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 
       
    // Isolate only the WARCWritables in this RDD
    var firstWARCs = warcInput.values

    // returns an RDD containing tuples of type (String, Array[String]) which represent an email and the array of pages where it was found sorted from least to greatest number of appearances.

    val source = sc.textFile("file:///Users/ethan/common-crawl-analytics/warc_file_list.txt")
    val length = source.count().toInt
    val lineArray = source.take(length).drop(1)

    // First specify an RDD that has all of our WARC objects, we will do the analyze/groupBy operations later
    for(dirPath <-lineArray){
      println("Directory: " + dirPath)
      val newPath = warcPathFirstHalf + dirPath

      firstWARCs.union(sc.newAPIHadoopFile(newPath, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]).values)
    }

    // now that we have all of our input files, do the actual analytic
    val newDF = firstWARCs.flatMap( warc => analyze4(warc.getRecord)).toDF("email","url")
    val reducedDF = newDF.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")

    // why do we need to sort here?
    // original:
    // val sorted = reduced.sortBy(_._2.size).map(tup => (tup._1, tup._2.mkString(",")))
    // val dataframesorted = sorted.toDF()
    // if we want sorting again (not tested):
    // newReducedDF = newDF.groupBy("email").agg(collect_set("url")) as "urlString").orderBy(size(col("urlString"))).withColumn("pageString",concat_ws(",", col("urlString")))

    println(reducedDF.count)

    val savedFilePath = "file:///Users/ethan/common-crawl-analytics/test.out"
    
    //firstDF.rdd.repartition(1).saveAsTextFile(savedFilePath)
    // firstDF.rdd.saveAsTextFile(savedFilePath)

    reducedDF.rdd.saveAsTextFile(savedFilePath)

    println("--------------")
    println(s"Emails found in WARCRecords saved in $savedFilePath")
    println("--------------")
    spark.stop()
  }

  def analyze(record: WARCRecord): Array[(String, Array[String])] = {
    val emails = returnEmails(record)
    if (emails.isEmpty) {
      return Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost()
      for (email <- emails) yield {
       (email, Array(url.toString))
      }
    }
  }

  val analyze3: (WARCRecord => Array[Tuple2[String, String]]) = (record: WARCRecord) => {
    var emails = Array[String]()
    val content = record.toString.split(" ")
    for (word <- content){
      if (word.length <= 126 && word.contains("@") && word.endsWith(".ic.gov")) word +: emails
    }
    if (emails.isEmpty) {
      Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost()
      for (email <- emails) yield {
        (email, url.toString)
      }
    }
  }

  val analyze4: (WARCRecord => Array[Tuple2[String, String]]) = (record: WARCRecord) => {
    val emails = record.toString.split(" ").filter(word => word.length <= 126 && word.contains("@") && word.endsWith(".ic.gov"))
    
    if (emails.isEmpty) {
      Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost()
      for (email <- emails) yield {
        (email, url.toString)
      }
    }
  }

  def analyze2(record: WARCRecord): Array[Tuple2[String, String]] = {

    // val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    // val milEmailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    // TODO: make this statically defined or global so we don't have to instantiate a new one every time
    val milEmailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.)mil\b""")

    // TODO: avoid doing a String copy here ... what does getContent return?
    val content = new String(record.getContent)

    val emails = milEmailPattern.findAllMatchIn(content).toArray.map(email => email.toString) //.filter(email=>email.endsWith(".mil"))

    if (emails.isEmpty) {
      return Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost()
      for (email <- emails) yield {
        (email, url.toString)
      }
    }
  }
  // TODO: see if we can create a new column in our DataFrame earlier. **For now, it is way easier to use an RDD then convert so we don't have to deal with a DF with Arrays
  // val analyze2Udf = udf(analyze2(_:WARCRecord))

  def returnEmails(record: WARCRecord): Array[String] = {

    val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    
    val content = new String(record.getContent)

    return emailPattern.findAllMatchIn(content).toArray.map(email => email.toString).filter(email=>email.endsWith(".mil"))
  }
}

