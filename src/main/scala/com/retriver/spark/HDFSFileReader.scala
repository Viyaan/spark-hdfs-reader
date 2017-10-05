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
     * argument 2 - Gateway Id
     * argument 3 - Start Date time
     * argument 4 - End Date time
     */
 
object HDFSFileReader {
 
  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    println("Retrive data from HDFS...")
    var query =""
    if(args.length > 1){
      if(args(2) !=null){
         query = "from.uuid = "+ args(2)
      }
      if(args(3) !=null){
        query =query + " and rxDateTime > "+ args(3)
      }
      if(args(4) !=null){
        query =query + " and rxDateTime < "+ args(4)
      }
    }
   
    println("Query to fetch data:  "+query)
    val conf = new org.apache.spark.SparkConf().setMaster("local[*]").setAppName("fdl-hdfs-reader")
    conf.set("fs.defaultFS", "hdfs://lolz1186.lol.de.bosch.com:9000")
    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var gateway =sqlContext.read.json(args(0))
    gateway.show()
    gateway.printSchema()
   
    if(query.length()!=0){
     gateway.filter(query)
    }
    gateway.rdd.saveAsTextFile(args(1))
  }
}