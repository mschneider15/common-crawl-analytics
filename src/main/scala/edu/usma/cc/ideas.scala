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

    val firstDir = "s3://commoncrawl/crawl-data/CC-MAIN-2016-18/segments/1461860106452.21/wet/CC-MAIN-20160428161506-00000-ip-10-239-7-51.ec2.internal.warc.wet.gz"
    val warcPathFirstHalf = "s3://commoncrawl/"


    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Open the firstDir directory as an RDD of type [(LongWritable, WARCWritable)]
    val warcInput = sc.newAPIHadoopFile(firstDir, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 
       
    // Isolate only the WARCWritables in this RDD
    var firstWARCs = warcInput.values

	val newDF = firstWARCs.flatMap( warc => analyze(warc.getRecord)).toDF("email","url")
    var reducedDF = newDF.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")
	reducedDF = reducedDF.localCheckpoint
	
    val source = sc.textFile("s3://eecs-practice/spark-test/wet2018.paths")
    val length = source.count().toInt
    val lineArray = source.take(length).drop(1)

    for(dirPath <-lineArray){
	  try{
      val newPath = warcPathFirstHalf + dirPath
	  val newValues = sc.newAPIHadoopFile(newPath, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]).values
	  val newDF = newValues.flatMap( warc => analyze(warc.getRecord)).toDF("email","url").groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")
	  reducedDF = reducedDF.union(newDF)
	  reducedDF = reducedDF.localCheckpoint
	  }  
	catch {e => print("error reading this warc file, file skipped"}
	}

    val savedFilePath = "s3://commoncrawltake2/ic" 
    reducedDF.rdd.repartition(8).saveAsTextFile(savedFilePath)


    spark.stop()
  }

    def analyze(record: WARCRecord): Array[Tuple2[String, String]] = {

    // TODO: can we make this statically defined or global so we don't have to instantiate a new one every time
    val milEmailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@.ic.gov\b""")

    val content = new String(record.getContent)

    val emails = milEmailPattern.findAllMatchIn(content).toArray.map(email => email.toString)
    var final_array:Array[(String,String)]= Array()
    
    if (emails.isEmpty) {
      return Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      url = uri.toURL.getHost().toString
      for (email <- emails)  {
        final_array = final_array :+ ((email, url))
      }
      }
	 }
