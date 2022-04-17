package test_env

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test_sql {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()

    spark.close()
  }
}
