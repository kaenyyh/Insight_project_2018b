package com.insight

import java.text.SimpleDateFormat

import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.text.SimpleDateFormat



object App {

  // change the string of date to timestamp
  def getTimestamp(x:Any) : Timestamp = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    if (x.toString() == "")
      return null
    else {
      val d = format.parse(x.toString());
      val t = new Timestamp(d.getTime());
      return t
    }
  }



  def main(args: Array[String]): Unit = {

    // only setting app name, all other properties will be specified at runtime for flexibility
    val conf = new SparkConf().setAppName("cassandra-example-hello")

    val sc = new SparkContext(conf)

    /*
      read from different sources:
    */

    // read from txt file contains data: {'revision', 'article_id', 'rev_id', 'article title', 'timestamp', 'date', 'time', 'username', 'userid'
    val myFile = sc.textFile("test2.txt")

    // read from s3:
    //    val myFile = sc.textFile("s3a://public-test-insight/input2.txt")


    /*
      Algorithm
     */

    //!!!!!!!!!!!!!!!  pass all columns in file into cassandra  !!!!!!!!!!!!!!!!
//    val TABLE_COLUMNS = SomeColumns("revision", "artid", "revid", "arttitle", "revtime", "date", "time", "username", "userid")
//
//    myFile.map{ line => {
//      val lines = line.split(" ")
//      val formatedtime = lines(4).slice(0,10) + " " + lines(4).slice(11,19)
//
//      // !!!!!!!!!!!!   convert string to timestamp !!!!!!!!!!!!!
//      //val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
//      //val date1 = simpleDateFormat.parse(lines(4).slice(0,10)).getTime
//      //val df = new SimpleDateFormat("yyyy-mm-dd")
//
//      (lines(0), lines(1), lines(2), lines(3), formatedtime, lines(4).slice(0,10), lines(4).slice(11,19), lines(5), lines(6))
//      }}.saveToCassandra("playground", "prodtest", TABLE_COLUMNS)


    //!!!!!!!!!!!!!!!!  mapreduce to calculate article update times by month !!!!!!!!!!!
    val TABLE_COLUMNS = SomeColumns("arttitle", "date", "count")

    myFile.map{ line => {
      val lines = line.split(" ")
      val formatedmonth = lines(4).slice(0,7)
      ((lines(3), formatedmonth), 1)
    }}.reduceByKey(_ + _).map(p => (p._1._1, p._1._2, p._2))
      .saveToCassandra("playground", "sumtest", TABLE_COLUMNS)


    // read from cassandra:
    //val hello = sc.cassandraTable[(String, Long)]("playground", "testtable")
    //val first = hello.first

    sc.stop

    println("it should print something!!!!!!")
    //println(hello)
    //println(first)

  }

  //////////// map
  ////////////
//  val TEXT: String = "input.txt"
//  val TABLE_COLUMNS = SomeColumns("id", "count")
//
//
//  def main (args: Array[String]) {
//
//    val conf = new SparkConf(true)
//      .setAppName("write_text_to_cassandra")
//      .set("spark.cassandra.connection.host", "ec2-34-213-54-16.us-west-2.compute.amazonaws.com")
//
//    val sc = new SparkContext(conf)
//
//    sc.textFile(TEXT)
//      .zipWithIndex()
//      .map{case (line,index) => {
//        val lines = line.split(",")
//
//        (lines(0), lines(1).toInt,  "")
//      }}
//      .saveToCassandra("playground", "stest", TABLE_COLUMNS)
//
//    sc.stop()
//  }

}
