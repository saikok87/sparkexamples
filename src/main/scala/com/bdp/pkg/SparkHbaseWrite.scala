package com.bdp.pkg

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.{Configuration => HBase}

//working program
object SparkHbaseWrite extends App {
  val sparkConf = new SparkConf().setAppName("HBaseWrite").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val conf = HBaseConfiguration.create()
  val tableName = args(0) // customer
  val textFile = args(1) // /warehouse/projects/EXCL/temp/cust.txt
  val parquetFile = args(2) // /warehouse/projects/EXCL/temp/cust_text.parquet

  conf.setInt("timeout", 120000)
  conf.set("hbase.zookeeper.quorum", "localhost")
  conf.set("zookeeper.property.clientPort", "2181")
  conf.set(TableInputFormat.INPUT_TABLE, tableName)

  //hbase connection


  /*import sqlContext.implicits._
  case class Customer(acc_id: Int, cust_name: String, acc_type: String, amount: Int)
  val cust_text = sc.textFile(args(0)).map(_.split(",")).map(c => Customer(c(0).trim.toInt, c(1), c(2), c(3).toInt)).toDF()
  cust_text.write.parquet(args(1))*/

  val cust_parquet = sqlContext.read.parquet(args(2))

  cust_parquet.foreach
    { r =>
      val conn = ConnectionFactory.createConnection(conf)
      val table = conn.getTable(TableName.valueOf(tableName))
      val row = new Put(Bytes.toBytes(r.getInt(0)))
      row.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cust_name"), Bytes.toBytes(r.getString(1)))
      row.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("acc_type"), Bytes.toBytes(r.getString(2)))
      row.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("amount"), Bytes.toBytes(r.getInt(3)))
      table.put(row)
      table.close
      conn.close
      }


}
