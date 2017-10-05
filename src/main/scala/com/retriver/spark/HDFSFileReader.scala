package com.retriver.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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
    /*
     * argument 0 - path of HDFS file or directory to read
     */
 
object HDFSFileReader extends App {
    val conf = new org.apache.spark.SparkConf().setMaster("local[*]").setAppName("fdl-hdfs-reader").set("fs.defaultFS", "hdfs://localhost:9000")
    var schema =new org.apache.spark.sql.SQLContext(new org.apache.spark.SparkContext(conf)).read.json(args(0))
    schema.show()
    schema.printSchema()
    schema.rdd.foreach(println)
}