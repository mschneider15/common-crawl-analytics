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


object SimpleApp {

  def main(args: Array[String]) {
		// Path to WARC files
		// Directory paths will allow access to all files within	
    val firstDir = "s3://commoncrawl/crawl-data/CC-MAIN-2016-18/segments/1461860106452.21/wet/CC-MAIN-20160428161506-00000-ip-10-239-7-51.ec2.internal.warc.wet.gz"
		val warcPathFirstHalf = "s3://commoncrawl/"

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
    val firstWARCs = warcInput.values
    
    // returns an RDD containing tuples of type (String, Array[String]) which represent an email and the array of pages where it was found sorted from least to greatest number of appearances.
    var firstRDD = firstWARCs.flatMap(warc => analyze(warc.getRecord)).filter( tup => tup._2 != null).reduceByKey(_ ++ _).sortBy(_._2.size).map(tup => (tup._1, tup._2.mkString(",")))
 	var firstDF = firstRDD.toDF
    
    val source = sc.textFile("s3://eecs-practice/spark-test/wet.paths")
    val length = source.count().toInt
    val lineArray = source.take(length).drop(1)
    for(dirPath <-lineArray){
      val newPath = warcPathFirstHalf + dirPath
      val newInput = sc.newAPIHadoopFile(newPath, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 
      
      val newWarcs = newInput.values
  
      // Creates a new RDD which contains tuples of an email and all of the pages it was found on. 
      val matches = newWarcs.flatMap( warc => analyze(warc.getRecord) )
    
      val filtered = matches.filter(tup => tup._2 != null)

      val reduced = filtered.reduceByKey(_ ++ _)

      val sorted = reduced.sortBy(_._2.size).map(tup => (tup._1, tup._2.mkString(",")))
      val dataframesorted = sorted.toDF()

      firstDF = firstDF.unionAll(dataframesorted)
    }
    val savedFilePath = "s3://eecs-practice/spark_test/test_10"
    
    //firstDF.rdd.repartition(1).saveAsTextFile(savedFilePath)
    firstDF.rdd.saveAsTextFile(savedFilePath)

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

  def returnEmails(record: WARCRecord): Array[String] = {

    val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    
    val content = new String(record.getContent)

    return emailPattern.findAllMatchIn(content).toArray.map(email => email.toString).filter(email=>email.endsWith(".mil"))
  }
}

