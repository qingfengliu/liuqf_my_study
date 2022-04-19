package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Spark07_sql_test {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    //准备数据。这里遇到权限问题.来自于hadoop.这里暂时简单讲hdfs权限放开忽略其蕴含的权限配置
    spark.sql(
      """
        |create table atguigu.user_visit_action(
        |data string,
        |user_id bigint,
        |session_id string,
        |page_id bigint,
        |action_time string,
        |search_keyword string,
        |click_category_id bigint,
        |click_product_id bigint,
        |order_category_ids string,
        |order_product_ids string,
        |pay_category_ids string,
        |pay_product_ids string,
        |city_id bigint
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    )
    spark.sql(
      """
        |load data local inpath 'D:/书籍资料整理/2.资料/spark-sql数据/user_visit_action.txt' into table atguigu.user_visit_action
        |""".stripMargin)
    spark.sql(
      """
        |create table atguigu.product_info(
        |product_id bigint,
        |product_name string,
        |extend_info string
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'D:/书籍资料整理/2.资料/spark-sql数据/product_info.txt' into table atguigu.product_info
        |""".stripMargin)

    spark.sql(
      """
        |create table atguigu.city_info(
        |city_id bigint,
        |city_name string,
        |area string
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'D:/书籍资料整理/2.资料/spark-sql数据/city_info.txt' into table atguigu.city_info
        |""".stripMargin)
    spark.sql("select * from atguigu.city_info").show()
    spark.close()
  }

}
