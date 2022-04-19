package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator


import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Spark09_sql_test2 {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use atguigu")
    //查询基本数据
    spark.sql(
      """
        |		select
        |				a.*,
        |				p.product_name,
        |				c.area,
        |				c.city_name
        |			from atguigu.user_visit_action a
        |			join atguigu.product_info p on a.click_product_id = p.product_id
        |			join atguigu.city_info c on a.city_id = c.city_id
        |				where a.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")
    //根据区域进行商品聚合
    spark.udf.register("cityRemark",functions.udaf(new CityRemarkUDAF()))
    spark.sql(
      """
        |select
        |area,
        |product_name,
        |count(*) as clickCnt,
        |cityRemark(city_name) as city_remark
        |from t1 group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")
    //区域内点击进行排行
    spark.sql(
      """
        |select *,rank() over(partition by area order by clickCnt desc) as rank
        |from t2
        |""".stripMargin
    ).createOrReplaceTempView("t3")
    //取前3名
    spark.sql(
      """
        |select *
        |from t3 where rank<=3
        |""".stripMargin).show(truncate = false)

    spark.close()
  }
  case class Buffer(var total:Long,var cityMap:mutable.Map[String,Long])
  //自定义聚合函数:实现城市备注功能
  //1.继承Aggregator,定义泛型.
  //IN:城市名称String
  //BUF:[总点击数,Map((city,cnt),(city,cnt))]
  //OUT:备注信息 String
  class CityRemarkUDAF extends Aggregator[String,Buffer,String]{
    //缓冲区初始化
    override def zero: Buffer = {
      Buffer(0,mutable.Map[String,Long]())
    }

    //更新缓冲区
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total+=1
      val newCnt=buff.cityMap.getOrElse(city,0L)+1
      buff.cityMap.update(city,newCnt)
      buff
    }

    //合并缓冲区数据
    override def merge(buffer1: Buffer, buffer2: Buffer): Buffer = {
      buffer1.total+=buffer2.total
      val map1=buffer1.cityMap
      val map2=buffer2.cityMap

      //两个Map合并
//      buffer1.cityMap=map1.foldLeft(map2) {
//        case (map, (city, cnt)) => {
//          val newCount = map.getOrElse(city,0L)+cnt
//          map.update(city,newCount)
//          map
//        }
//      }
      map2.foreach{
        case (city,cnt)=>{
          val newCount=map1.getOrElse(city,0L)+cnt
          map1.update(city,newCount)
        }
      }
      buffer1.cityMap=map1
      buffer1
    }

    //将统计结果生成字符串信息
    override def finish(buff: Buffer): String = {
      val remarkList=ListBuffer[String]()
      val totalcnt=buff.total
      val cityMap=buff.cityMap
      //降序排列
      val cityCntList=cityMap.toList.sortWith(
        (left,right)=>{
          left._2>right._2
        }
      ).take(2)

      val hasMore=cityMap.size>2
      var rsum=0L
      cityCntList.foreach{
        case (city,cnt)=>{
          val r=cnt*100/totalcnt
          remarkList.append(s"${city} ${r}%")
          rsum += r
        }
      }
      if (hasMore){
        remarkList.append(s"其他 ${100-rsum}%")
      }
      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
