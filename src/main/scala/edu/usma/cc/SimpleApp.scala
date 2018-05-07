package edu.usma.cc

import java.io._
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.util.Collection
import java.util.Iterator
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
		
		// Initialize the sparkSession
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
		

		// Open warcFile as an input stream
    val warcInput = sc.newAPIHadoopFile(warcFile, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 

		// Read in the file directories for analysis
    // val logData = spark.read.textFile(firstDir, secondDir)
    
    
    // Save all emails as a text file in the following directory.
    // TODO - Collect results so they are automatically stored in a single file
    // emails.saveAsTextFile("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")

    val warcs = warcInput.values

    val test1Result = warcs.map( warc => analyze(warc.getRecord) )

    // Initiate the records information

    println("--------------")
    println("WARCRecords:")
    test1Result.foreach(println)
    spark.stop()
  }

  def analyze(record: WARCRecord): String = {
    val ret = returnEmails(record)
    return ret
  }

  def returnEmails(record: WARCRecord): String = {
		// Defines the Regex used in finding emails
    val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    
    val content = new String(record.getContent)

    var ret = ""
    emailPattern.findAllMatchIn(content).map(_ + "\n").foreach(ret += _)
    return ret
  }

  def returnRecordHeader(record: WARCRecord): String = {
    var ret = "--------------\n"
    ret += "     Record-ID: " + record.getHeader.getRecordID + "\n"
    ret += "          Date: " + record.getHeader.getDateString + "\n"
    ret += "Content-Length: " + record.getHeader.getContentLength + "\n"
    ret += "  Content-Type: " + record.getHeader.getContentType + "\n"
    ret += "     TargetUri: " + record.getHeader.getTargetURI + "\n"
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
