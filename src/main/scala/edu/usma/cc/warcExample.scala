package edu.usma.cc

import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.util.Collection
import java.util.Iterator
 
import org.jwat.warc.WarcReader
import org.jwat.warc.WarcReaderFactory
import org.jwat.warc.WarcRecord

import org.apache.spark.sql.SparkSession

object TestWarc {
 
  val warcFile = "/Users/andre/Documents/Academics/AY18-2/cs489A/crawl-data/CC-MAIN-2018-05/segments/1516084887660.30/wet/CC-MAIN-20180118230513-20180119010513-00000.warc.wet.gz";
 
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Test WARC").getOrCreate()
    val file = new File( warcFile )
    val in = new FileInputStream( file )
 
    var records = 0
 
    val reader = WarcReaderFactory.getReader( in )
    var record = reader.getNextRecord()
    while (records <= 20) {
      printRecord(record)
      record = reader.getNextRecord()
      records += 1
    }
 
    println("--------------");
    println("       Records: " + records);
    reader.close();
    in.close();
    spark.stop()
  }
 
  def printRecord(record: WarcRecord) {
    println("--------------")
    println("       Version: " + record.header.bMagicIdentified + " " + record.header.bVersionParsed + " " + record.header.major + "." + record.header.minor)
    println("       TypeIdx: " + record.header.warcTypeIdx)
    println("          Type: " + record.header.warcTypeStr)
    println("      Filename: " + record.header.warcFilename)
    println("     Record-ID: " + record.header.warcRecordIdUri)
    println("          Date: " + record.header.warcDate)
    println("Content-Length: " + record.header.contentLength)
    println("  Content-Type: " + record.header.contentType)
    println("     Truncated: " + record.header.warcTruncatedStr)
    println("   InetAddress: " + record.header.warcInetAddress)
    println("      RefersTo: " + record.header.warcRefersToUri)
    println("     TargetUri: " + record.header.warcTargetUriUri)
    println("   BlockDigest: " + record.header.warcBlockDigest)
    println(" PayloadDigest: " + record.header.warcPayloadDigest)
    println("IdentPloadType: " + record.header.warcIdentifiedPayloadType)
    println("       Profile: " + record.header.warcProfileStr)
    println("      Segment#: " + record.header.warcSegmentNumber)
    println(" SegmentOrg-Id: " + record.header.warcSegmentOriginIdUrl)
    println("SegmentTLength: " + record.header.warcSegmentTotalLength)
  }
}

