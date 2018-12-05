package hive

import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf


/**
  * Created with IntelliJ IDEA.
  * User: tangjuhong
  * Date: 2018/9/13
  * Time: 上午10:39
  **/
object md5udaf {
  def md5logic(str:String):String=
  {
     var result:String="";
    import org.apache.commons.codec.digest.DigestUtils
    result = DigestUtils.md5Hex(str)
    return  result;
  }


  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[1]").appName("UDAFTest").getOrCreate()
    spark.udf.register("mymd5",md5logic(_:String))
    /*spark.udf.register("mymd5",(x:String)=>{
      import org.apache.commons.codec.digest.DigestUtils
      DigestUtils.md5Hex(x)
    })*/
    val json= spark.read.json("/Users/tangjuhong/kuainiu/sourcecode/SparkStudy/main/resources/employees.json")
    json.createOrReplaceTempView("employees")
    json.show();
    spark.sql("select  id,name,mymd5(concat(name,id)) as md5value from employees").show()
    println("Query Success")

  }
}
