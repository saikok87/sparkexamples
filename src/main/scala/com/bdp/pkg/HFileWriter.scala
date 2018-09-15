package com.bdp.pkg

import com.bdp.pkg.SparkHbaseWrite.conf
import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles

object HFileWriter extends App {
  val conf = HBaseConfiguration.create()
  val tableName = args(0) //customer
  val parquet_file = args(1) // /warehouse/projects/EXCL/temp/cust_text.parquet
  val pathToHFile = args(2) // /warehouse/projects/EXCL/temp/
  val table = new HTable(conf, tableName)
  val sparkConf = new SparkConf().setAppName("HFileWriter").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  conf.setInt("timeout", 120000)
  conf.set("hbase.zookeeper.quorum", "localhost")
  conf.set("zookeeper.property.clientPort", "2181")
  conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
  val job = Job.getInstance(conf)
  job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setMapOutputValueClass(classOf[KeyValue])
  HFileOutputFormat.configureIncrementalLoad(job, table)

  val parquetDf = sqlContext.read.parquet(args(1))

  val rdd = parquetDf.rdd.map(r => {
    val kv: KeyValue = new KeyValue(Bytes.toBytes(r.getInt(0)), "cf1".getBytes(), "cust_name".getBytes(), Bytes.toBytes(r.getString(1)))
    (new ImmutableBytesWritable(Bytes.toBytes(r.getInt(0))), kv)
  })

  // Directly bulk load to Hbase/MapRDB tables.
  rdd.saveAsNewAPIHadoopFile(pathToHFile, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], job.getConfiguration())

}
