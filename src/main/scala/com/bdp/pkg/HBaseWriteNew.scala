
package com.bdp.pkg

import com.bdp.pkg.SparkHbaseWrite.args
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object HBaseWriteNew {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local[2]")
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
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(tableName))

    import sqlContext.implicits._

    val cust_parquet = sqlContext.read.parquet(args(2))

    //cust_parquet.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog))

  }

  def catalog = s"""{
                   |"table":{"namespace":"default", "name":"customer"},
                   |"rowkey":"acc_id",
                   |"columns":{
                   |"rowkey":{"cf":"rowkey", "col":"acc_id", "type":"int"},
                   |"officeAddress":{"cf":"cf1", "col":"cust_name", "type":"string"},
                   |"officePhone":{"cf":"cf1", "col":"acc_type", "type":"string"},
                   |"personalName":{"cf":"cf1", "col":"amount", "type":"int"}
                   |}
                   |}""".stripMargin

}
