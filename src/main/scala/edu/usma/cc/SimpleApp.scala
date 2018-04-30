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
import org.apache.hadoop.io.LongWritable

object SimpleApp {

  def main(args: Array[String]) {
		// Path to WARC files
		// Directory paths will allow access to all files within	
  	val warcFile = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084887660.30/wet/CC-MAIN-20180118230513-20180119010513-00000.warc.wet.gz"
    val firstDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084887660.30/wet/*"
    val secondDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084891105.83/wet/*"
		
		// Initialize the sparkSession
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
		
    // Initialize a StreamingContext, ssc
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

		// Open warcFile as an input stream
    // format: fileStream[keyClass, valueClass, InputFormatClass](filePath)
    val in = ssc.fileStream[LongWritable,WARCWritable,WARCInputFormat](warcFile) 

		// Read in the file directories for analysis
    // val logData = spark.read.textFile(firstDir, secondDir)
    
		// Defines the Regex used in finding emails
    // val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    
		// Find all of the emails in each line of the warc files
		// val emails = logData.rdd.flatMap{line => emailPattern.findAllMatchIn(line)}
    
    // Save all emails as a text file in the following directory.
    // TODO - Collect results so they are automatically stored in a single file
    // emails.saveAsTextFile("/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-results/emails.txt")

    // Initiate the records information

    println("--------------")
    println("WARCRecords:")
    val urls = in.mapValues(
        record => (record.getRecord.getHeader.getTargetURI, 1)
    )
    //val urlsCounts = urls.reduceByKey(_ + _)
    //urlsCounts.print()
    urls.print()
    ssc.start()
    ssc.awaitTermination()
    // Shut down the Spark session
    if ssc.stop()
    // spark.stop()
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
