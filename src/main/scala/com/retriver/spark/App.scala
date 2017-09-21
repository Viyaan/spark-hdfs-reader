package com.retriver.spark

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.SparkContext

object App {

  def main(args: Array[String]) {
    println("Trying to write file in HDFS...")
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(conf)
    val output = fs.create(new Path("/dummy_storage/tmp/mySample.txt"))
    val writer = new PrintWriter(output)
    try {
      writer.write("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}")
      writer.write("\n")
    } finally {
      writer.close()
      println("Closed!")
    }
    println("Done!")
    
   println("Trying to Read file from HDFS...")
    val conf1 = new SparkConf().setMaster("local[*]").setAppName("dataset").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf1)

    val lines = sc.textFile("hdfs://localhost:9000/dummy_storage/tmp/mySample.txt")

  }

}