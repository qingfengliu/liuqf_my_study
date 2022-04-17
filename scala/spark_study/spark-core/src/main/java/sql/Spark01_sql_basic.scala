package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01_sql_basic {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    val df=spark.read.json("datasets/user.json")
//    df.show()

    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show

    df.select("age","username").show
    //在使用dataframe涉及到转换操作需要引入转换规则
    import spark.implicits._
    df.select($"age"+1).show
    spark.close()
  }
}
