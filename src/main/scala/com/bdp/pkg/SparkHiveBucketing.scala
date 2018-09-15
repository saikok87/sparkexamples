package com.bdp.pkg

import org.apache.spark.{SparkConf, SparkContext}

//working program
object SparkHiveBucketing extends App {
  val sparkConf = new SparkConf().setAppName("HiveBucketing").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
  hiveContext.setConf("hive.enforce.bucketing","true")
  //reading from textfile
  /*val userInfoFile = sc.textFile("/warehouse/projects/EXCL/source/user_info.txt")
  case class UserInfo(user_id:Long, firstname:String, lastname:String, ds: String)
  import hiveContext.implicits._
  import scala.collection.JavaConverters._
  val userInfoDF = userInfoFile.map(_.split(",")).map(e => UserInfo(e(0).toLong, e(1), e(2), e(3))).toDF()
*/
  //writing to temp table
  //userInfoDF.write.partitionBy("ds").mode(org.apache.spark.sql.SaveMode.Append).insertInto("e_excl_db.user_info_bucketed")

  //insert to bucketed table
  hiveContext.sql("INSERT OVERWRITE TABLE e_excl_db.user_info_bucketed_new partition(ds) SELECT * from e_excl_db.user_info_bucketed")

}
