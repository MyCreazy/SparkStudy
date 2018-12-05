package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * User: tangjuhong
  * Date: 2018/9/4
  * Time: 上午11:24
  **/
object HiveToHbaseApplication {
  /**
    * main函数入口
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    /////注意这种做法需要先把hbase表创建好,然后把hive的数据导入hbase
    val hbaseconf = HBaseConfiguration.create()
    val jobconf = new JobConf(hbaseconf)
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, "test:test789")

    val sparkconf = new SparkConf().setAppName("hivetohbase").setMaster("local")
    val sc = new SparkContext(sparkconf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("select CONCAT(token_auth_id,token_expires) as rowkey,token_status   from  ods.testhbase").show()
    val datalist = hiveContext.sql("select CONCAT(token_expires,token_auth_id) as rowkey,token_status   from  ods.testhbase")
    var hbaserdd = datalist.rdd.map(row => {
      val rowkey = row(0).asInstanceOf[String]
      val token_expires = row(1).asInstanceOf[Int]

      val p = new Put(Bytes.toBytes(rowkey.toString))

      p.add(Bytes.toBytes("xx"), Bytes.toBytes("token_expires"), Bytes.toBytes(token_expires.toString))
      (new ImmutableBytesWritable, p)
    })

    hbaserdd.saveAsHadoopDataset(jobconf)
    println("数据写入完成")
  }
}
