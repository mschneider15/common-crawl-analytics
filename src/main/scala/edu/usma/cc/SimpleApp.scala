package edu.usma.cc

import java.io._
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/p/work1/kyleking/README.md"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    
    val result = s"Lines with a: $numAs, Lines with b: $numBs"
    new PrintWriter(new File("/p/work1/kyleking/results.txt")) { write(result); close }
    spark.stop()
  }
}
