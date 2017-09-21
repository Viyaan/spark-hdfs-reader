package com.bosch.tt.icom.retriver.spark

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import sys.process.stringSeqToProcess
import org.apache.spark.SparkConf 
import org.apache.spark.rdd.RDD 
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._

object HDFSFileReader {

  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    println("Trying to read file from HDFS...")
    val conf = new SparkConf().setMaster("local[*]").setAppName("HDFSFileReader")
    conf.set("fs.defaultFS", "hdfs://LOLZ1186.lol.de.bosch.com:9000")
    val sc = new SparkContext(conf)
    val data = sc.textFile("hdfs://localhost:9000/dummy_storage/ImpalaTesdata/2017/9/18").toDF
    data.saveAsTextFile("\\usr\\share\\spark")
    
   
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    var gateway =sqlContext.jsonFile("hdfs://localhost:9000/dummy_storage/ImpalaTesdata/2017/9/18/111111252.json")
    gateway.show()
    gateway.printSchema()
    val b = sqlContext.sql("select * from gateway")
    b.collect.foreach(println)
  }
}