package dataframes

import java.sql.Timestamp
import java.util.UUID

import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra.{CassandraSourceRelation, DataFrameReaderWrapper, DataFrameWriterWrapper}
import org.apache.spark.sql.functions.{avg, count, max}

import scala.collection.mutable.ListBuffer

object AudienceDump extends App {

  val ss = SparkSession.builder().config("spark.cassandra.connection.host", "localhost").master("local[*]").getOrCreate()
  import ss.implicits._




//
//  var tempertaureByDays = new ListBuffer[TempertaureByDay]()
//  println("start dumping "+new java.util.Date())
//  val rnd = new scala.util.Random
//  1 to 3000000 foreach(_ => {
//    tempertaureByDays += TempertaureByDay(UUID.randomUUID().toString, 2 + rnd.nextInt(38),
//    new java.sql.Timestamp(new java.util.Date().getTime), s"2018-${1+rnd.nextInt(11)}-${1+rnd.nextInt(30)}")
//  })
//
//
//
//  val tempertaureByDayRDD = ss.sparkContext.parallelize(tempertaureByDays)
//  val tempertaureByDayDF = ss.createDataFrame(tempertaureByDayRDD)
//  tempertaureByDayDF.write
//    .cassandraFormat("temperature_by_day", "smart_home", CassandraSourceRelation.defaultClusterName, true)
//    .mode(SaveMode.Append)
//    .save()
//
//  println("end dumping "+new java.util.Date())


  println(new java.util.Date())
  val tempertaureByDayDF = ss
    .read
    .cassandraFormat("temperature_by_day", "smart_home")
    .options(ReadConf.SplitSizeInMBParam.option(256))
    .load()


  tempertaureByDayDF.select(avg('temperature), count('temperature), max('temperature)).show
  println(new java.util.Date())





}
