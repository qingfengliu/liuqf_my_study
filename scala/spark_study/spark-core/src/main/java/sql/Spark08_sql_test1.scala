package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Spark08_sql_test1 {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    //准备数据。这里遇到权限问题.来自于hadoop.这里暂时简单讲hdfs权限放开忽略其蕴含的权限配置

    spark.sql("use atguigu")
    spark.sql(
      """
        |select *
        |from
        |(
        |	select
        |	*,
        |	rank() over(partition by area order by clickCnt desc) as rank
        |	from
        |	(
        |		select
        |		area,
        |		product_name,
        |		count(*) as clickCnt
        |		 from
        |		(
        |			select
        |				a.*,
        |				p.product_name,
        |				c.area,
        |				c.city_name
        |			from atguigu.user_visit_action a
        |			join atguigu.product_info p on a.click_product_id = p.product_id
        |			join atguigu.city_info c on a.city_id = c.city_id
        |			where a.click_product_id > -1
        |		)t1
        |		group by area,product_name
        |	)t2
        |)t3 where rank <=3
        |""".stripMargin).show()
    spark.close()
  }

}
