package com.tjh.sparkstudy.clean

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by tjh.
  * Date: 2019/1/3 下午2:46
  **/
object AGradeExportToHive {
  def main(args: Array[String]): Unit = {
    ////读取hbase数据转换为rdd然后写入hive
    print("come in")
    val sparkConf = new SparkConf().setMaster("yarn").setAppName("AGradeExportToHive")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "snow:test1")
    val sc = new SparkContext(sparkConf)
    val sqlcontext = new SQLContext(sc)
    ///如果用饿sparksession则用sparksession
    import sqlcontext.implicits._
    val hbaseRdd=sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])

    val acard=hbaseRdd.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("userName"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("identity"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("phoneNumber"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("ageGrade"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("genderGrade"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("marryGrade"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("educationGrade"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("rentGrade"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("workTypeGrade"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("workTimeGrade"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("annualIncomeGrade"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("cf"),Bytes.toBytes("sumGrade")))
    )).toDF("userName","identity","phoneNumber","ageGrade","genderGrade","marryGrade","educationGrade","rentGrade","workTypeGrade","workTimeGrade","annualIncomeGrade","sumGrade")

    acard.registerTempTable("temp")
    acard.show(10)
    sqlcontext.sql("insert overwrite table tdm.tdm_s_acard_result1  partition(dt='20180101')  select *from temp")
  }
}
