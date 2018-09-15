package com.bdp.pkg


import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

//Working program
object SparkHbaseReader {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = args(0)

    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("Number of Records found : " + hBaseRDD.count())
    hBaseRDD.collect()

    //hbase connection
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(tableName))

    val scan = new Scan()
    val scanner: ResultScanner = table.getScanner(scan)
    import scala.collection.JavaConverters._
    scanner.asScala foreach println
    table.close
    scanner.close()

  }

}
