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
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io._

object SimpleApp {

  def main(args: Array[String]) {
		// Path to WARC files
		// Directory paths will allow access to all files within	
  	val warcFile = "/Users/andre/Documents/Academics/AY18-2/cs489A/CC-MAIN-20180118230513-20180119010513-00000.warc.wet"
    val firstDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084887660.30/wet/*"
    val secondDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084891105.83/wet/*"
    val warcDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/"
		
		// Initialize the sparkSession
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
		

		// Open warcFile as an input stream
    val warcInput = sc.newAPIHadoopFile(firstDir, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 

		// Read in the file directories for analysis
    // val logData = spark.read.textFile(firstDir, secondDir)
    
    
    // Save all emails as a text file in the following directory.
    // TODO - Collect results so they are automatically stored in a single file
    // emails.saveAsTextFile("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")

    val warcs = warcInput.values
    
    // Creates a new RDD which contains tuples of matched emails and the page they were found on. 
    val matches = warcs.flatMap( warc => analyze(warc.getRecord) )
    
    val filtered = matches.filter(tup => tup._2 != null)

    val reduced = filtered.reduceByKey(_ ++ _)

    val sorted = reduced.sortBy(_._2.size)

    sorted.saveAsTextFile("test1")

    // Initiate the records information

    println("--------------")
    println("WARCRecords save as test1")
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
