package edu.usma.cc

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
    val firstDir = "/p/work2/commoncrawl/crawl-data/CC-MAIN-2018-05/segments/1516084886237.6/wet/*"
		val warcPathFirstHalf = "/p/work2/commoncrawl/crawl-data/CC-MAIN-2018-05/segments/"

    // An array of the final path segments for each directory containing warc files
    val warcDirs = Array("1516084886397.2/wet/*",
      "1516084886416.17/wet/*",
      "1516084886436.25/wet/*",
      "1516084886437.0/wet/*",
      "1516084886476.31/wet/*",
      "1516084886639.11/wet/*",
      "1516084886739.5/wet/*",
      "1516084886758.34/wet/*",
      "1516084886792.7/wet/*",
      "1516084886794.24/wet/*",
      "1516084886815.20/wet/*",
      "1516084886830.8/wet/*",
      "1516084886860.29/wet/*",
      "1516084886895.18/wet/*",
      "1516084886939.10/wet/*",
      "1516084886946.21/wet/*",
      "1516084886952.14/wet/*",
      "1516084886964.22/wet/*",
      "1516084886979.9/wet/*",
      "1516084887024.1/wet/*",
      "1516084887054.15/wet/*",
      "1516084887065.16/wet/*",
      "1516084887067.27/wet/*",
      "1516084887077.23/wet/*",
      "1516084887224.19/wet/*",
      "1516084887253.36/wet/*",
      "1516084887414.4/wet/*",
      "1516084887423.43/wet/*",
      "1516084887535.40/wet/*",
      "1516084887600.12/wet/*",
      "1516084887621.26/wet/*",
      "1516084887660.30/wet/*",
      "1516084887692.13/wet/*",
      "1516084887729.45/wet/*",
      "1516084887746.35/wet/*",
      "1516084887832.51/wet/*",
      "1516084887849.3/wet/*",
      "1516084887973.50/wet/*",
      "1516084887981.42/wet/*",
      "1516084888041.33/wet/*",
      "1516084888077.41/wet/*",
      "1516084888113.39/wet/*",
      "1516084888135.38/wet/*",
      "1516084888302.37/wet/*",
      "1516084888341.28/wet/*",
      "1516084888878.44/wet/*",
      "1516084889325.32/wet/*",
      "1516084889473.61/wet/*",
      "1516084889542.47/wet/*",
      "1516084889567.48/wet/*",
      "1516084889617.56/wet/*",
      "1516084889660.55/wet/*",
      "1516084889677.76/wet/*",
      "1516084889681.68/wet/*",
      "1516084889733.57/wet/*",
      "1516084889736.54/wet/*",
      "1516084889798.67/wet/*",
      "1516084889917.49/wet/*",
      "1516084890187.52/wet/*",
      "1516084890314.60/wet/*",
      "1516084890394.46/wet/*",
      "1516084890514.66/wet/*",
      "1516084890582.77/wet/*",
      "1516084890771.63/wet/*",
      "1516084890795.64/wet/*",
      "1516084890823.81/wet/*",
      "1516084890874.84/wet/*",
      "1516084890893.58/wet/*",
      "1516084890928.82/wet/*",
      "1516084890947.53/wet/*",
      "1516084890991.69/wet/*",
      "1516084891105.83/wet/*",
      "1516084891196.79/wet/*",
      "1516084891277.94/wet/*",
      "1516084891316.80/wet/*",
      "1516084891377.59/wet/*",
      "1516084891485.97/wet/*",
      "1516084891530.91/wet/*",
      "1516084891539.71/wet/*",
      "1516084891543.65/wet/*",
      "1516084891546.92/wet/*",
      "1516084891705.93/wet/*",
      "1516084891706.88/wet/*",
      "1516084891750.87/wet/*",
      "1516084891791.95/wet/*",
      "1516084891886.70/wet/*",
      "1516084891926.62/wet/*",
      "1516084891976.74/wet/*",
      "1516084891980.75/wet/*",
      "1516084892059.90/wet/*",
      "1516084892238.78/wet/*",
      "1516084892699.72/wet/*",
      "1516084892802.73/wet/*",
      "1516084892892.85/wet/*",
      "1516084893300.96/wet/*",
      "1516084893397.98/wet/*",
      "1516084893530.89/wet/*",
      "1516084893629.85/wet/*",
      "1516084894125.99/wet/*")
		// Initialize the sparkSession
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
		

		// Open the firstDir directory as an RDD of type [(LongWritable, WARCWritable)]
    val warcInput = sc.newAPIHadoopFile(firstDir, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 
    
    // TODO - Collect results so they are automatically stored in a single file
    
    // Isolate only the WARCWritables in this RDD
    val firstWARCs = warcInput.values
    
    // returns an RDD containing tuples of type (String, Array[java.net.URI]) which represent an email and the array of pages where it was found sorted from least to greatest number of appearances.
    var firstRDD = firstWARCs.flatMap( warc => analyze(warc.getRecord) ).filter( tup => tup._2 != null ).reduceByKey(_ ++ _).sortBy(_._2.size)

    for (dirPath <- warcDirs) {
      val newPath = warcPathFirstHalf + dirPath
      val newInput = sc.newAPIHadoopFile(newPath, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 
      
      val newWarcs = newInput.values
      // Creates a new RDD which contains tuples of an email and all of the pages it was found on. 
      val matches = newWarcs.flatMap( warc => analyze(warc.getRecord) )
    
      val filtered = matches.filter(tup => tup._2 != null)

      val reduced = filtered.reduceByKey(_ ++ _)

      val sorted = reduced.sortBy(_._2.size)
     
      firstRDD = firstRDD.union(sorted)
    }

    val savedFilePath = "/p/work2/commoncrawl/crawl-results/object"

    val finalResults = firstRDD.sortBy(_._2.size)
    
    finalResults.saveAsObjectFile(savedFilePath)

    println("--------------")
    println(s"Emails found in WARCRecords saved in $savedFilePath")
    println("--------------")
    spark.stop()
  }

  def analyze(record: WARCRecord): Array[(String, Array[URI])] = {
    val emails = returnEmails(record)
    if (emails.isEmpty) {
      return Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      for (email <- emails) yield {
       (email, Array(uri))
      }
    }
  }

  def returnEmails(record: WARCRecord): Array[String] = {
		// Defines the Regex used in finding emails
    val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    
    val content = new String(record.getContent)

    return emailPattern.findAllMatchIn(content).toArray.map(email => email.toString)
  }

  def returnRecordHeader(header: Header): String = {
    var ret = "--------------\n"
    ret += "     Record-ID: " + header.getRecordID + "\n"
    ret += "          Date: " + header.getDateString + "\n"
    ret += "Content-Length: " + header.getContentLength + "\n"
    ret += "  Content-Type: " + header.getContentType + "\n"
    ret += "     TargetUri: " + header.getTargetURI + "\n"
    return ret
  }

  def printRecordHeader(record: WARCRecord) {
    println("--------------")
    println("     Record-ID: " + record.getHeader.getRecordID)
    println("          Date: " + record.getHeader.getDateString)
    println("Content-Length: " + record.getHeader.getContentLength)
    println("  Content-Type: " + record.getHeader.getContentType)
    println("     TargetUri: " + record.getHeader.getTargetURI)
  }
}
