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

import org.jwat.warc.WarcReader
import org.jwat.warc.WarcReaderFactory
import org.jwat.warc.WarcRecord

import org.apache.spark.sql.SparkSession

  def main(args: Array[String]) {
		// Path to WARC files
		// Directory paths will allow access to all files within	
  	val warcFile = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084887660.30/wet/CC-MAIN-20180118230513-20180119010513-00000.warc.wet.gz"
    val firstDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084887660.30/wet/*"
    val secondDir = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084891105.83/wet/*"
		
		// Open warcFile as an input stream
    // TODO - Open warcFile as an input stream within Spark
		val file = new File( warcFile )
	  val in = new FileInputStream( file )
    
		// Initialize the sparkSession
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
		
    // Initialize a StreamingContext, ssc
    // val ssc = new StreamingContext(spark)

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
    var records = 0
    var reader = WarcReaderFactory.getReader( in )
    var record = reader.getNextRecord()
    while ( records <= 20 ) {
      printRecord(record)
      record = reader.getNextRecord()
      records += 1
    }

    println("--------------")
    println("       Records: " + records)
    reader.close()
    in.close()

    // Shut down the Spark session
    spark.stop()
  }

  def printRecord(record: WarcRecord) {
    println("--------------")
    println("       Version: " + record.header.major + "." + record.header.minor)
    println("       TypeIdx: " + record.header.warcTypeIdx)
    println("          Type: " + record.header.warcTypeStr)
    println("      Filename: " + record.header.warcFilename)
    println("     Record-ID: " + record.header.warcRecordIdUri)
    println("          Date: " + record.header.warcDate)
    println("Content-Length: " + record.header.contentLength)
    println("  Content-Type: " + record.header.contentType)
    println("      RefersTo: " + record.header.warcRefersToUri)
    println("     TargetUri: " + record.header.warcTargetUriUri)
    println("   BlockDigest: " + record.header.warcBlockDigest)
  }
}
