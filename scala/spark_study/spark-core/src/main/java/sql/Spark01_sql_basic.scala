package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


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

    //TODO DataSet
    val seq=Seq(1,2,3,4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df1: DataFrame = rdd.toDF("id", "name", "age")

    //rdd=>dafaframe
    df1.show()
    val rowRDD:RDD[Row]= df1.rdd

    val ds1: Dataset[User] = df1.as[User]
//    //dafaframe<=>dataset
//
    val df2: DataFrame = ds1.toDF()
    df2.show()

//    //RDD<=>DataSet
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val userRDD:RDD[User]=ds2.rdd
    userRDD.collect().foreach(println)
    spark.close()
  }
  case class User(id:Int,name:String,age:Int)
}
