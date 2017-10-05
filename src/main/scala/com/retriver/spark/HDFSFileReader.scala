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
     * argument 1 - path where file is stored in destination
     */
 
object HDFSFileReader extends App {
 
    println("Retrive data from HDFS...")
    val conf = new org.apache.spark.SparkConf().setMaster("local[*]").setAppName("fdl-hdfs-reader")
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    val sqlContext = new org.apache.spark.sql.SQLContext(new org.apache.spark.SparkContext(conf))
    var schema =sqlContext.read.json(args(0))
    schema.show()
    schema.printSchema()
    schema.rdd.saveAsTextFile(args(1))
}