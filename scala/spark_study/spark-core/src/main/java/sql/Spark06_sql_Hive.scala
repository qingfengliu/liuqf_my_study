package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}


object Spark06_sql_Hive {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    spark.sql("show databases")
    spark.sql("use default")
    spark.sql("show tables").show()

    spark.close()
  }

}
