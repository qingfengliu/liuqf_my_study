package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object Spark02_sql_UDF {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df=spark.read.json("datasets/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName",(name:String)=>{
      "Name:"+name
    })
    spark.sql("select age,prefixName(username) from user").show()


    spark.close()
  }

}
