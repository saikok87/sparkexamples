package com.bdp.pkg

import org.apache.spark.{SparkConf, SparkContext}

//working program
//appending data to hive partition table
object SparkHiveExample extends App {

  val sparkConf = new SparkConf().setAppName("HFileWriter").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
  val empFile = sc.textFile("/warehouse/projects/EXCL/source/emp.txt")
  case class employee(id:Int, name:String, dept:String, year: Int)
  import hiveContext.implicits._
  val empDF = empFile.map(_.split(",")).map(e => employee(e(0).toInt, e(1), e(2), e(3).toInt)).toDF()
  empDF.write.partitionBy("year").mode(org.apache.spark.sql.SaveMode.Append).insertInto("e_excl_db.emp_tab")

}
