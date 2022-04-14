package test_env

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//日期 用户ID Session_id 页面ID 动作时间 搜索关键字
object Spark03_Req1_HotCatoryTop10Analysis1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc = new SparkContext(sparkconf)
    //获取原始数据
    //Q1 actionRDD重复使用
    //TODO TOP10热门品类
    val actionRDD = sc.textFile("D:\\packages\\user_visit_action.txt")
    actionRDD.cache()

    //统计品类的点击数量
    val clickActionRDD = actionRDD.filter(action => {
      val datas = action.split("_")
      datas(6) != "-1"
    })

    val clickCountRDD = clickActionRDD.map(action => {
      val datas = action.split("_")
      (datas(6), 1)
    }).reduceByKey(_ + _)

    //统计品类的下单数量
    val orderActionRDD = actionRDD.filter(action => {
      val datas = action.split("_")
      datas(8) != "null"
    })
    //orderid=> 1,2,3
    // (1,1) (2,1) (3,1)
    val orderCountRDD=orderActionRDD.flatMap(
      action=>{
        val datas = action.split("_")
        val cid=datas(8)
        val cids=cid.split(",")
        cids.map(id=>(id,1))
      }
    ).reduceByKey(_+_)

    //4.统计品类的支付数量

    val payActionRDD = actionRDD.filter(action => {
      val datas = action.split("_")
      datas(10) != "null"
    })
    //orderid=> 1,2,3
    // (1,1) (2,1) (3,1)
    val payCountRDD=payActionRDD.flatMap(
      action=>{
        val datas = action.split("_")
        val cid=datas(10)
        val cids=cid.split(",")
        cids.map(id=>(id,1))
      }
    ).reduceByKey(_+_)

    val rdd1=clickCountRDD.map{
      case (cid,cnt)=>{
        (cid,(cnt,0,0))
      }
    }

    val rdd2=orderCountRDD.map{
      case (cid,cnt)=>{
        (cid,(0,cnt,0))
      }
    }
    val rdd3=payCountRDD.map{
      case (cid,cnt)=>{
        (cid,(0,0,cnt))
      }
    }
    //将三个数据源合并在一起,然后进行聚合计算
    val sourceRDD= rdd1.union(rdd2).union(rdd3)
    val analysisRDD=sourceRDD.reduceByKey(
      (t1,t2)=>{
        (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
      }
    )
    val resultRDD=analysisRDD.sortBy(_._2,false).take(10)
    resultRDD.foreach(println)
  }
}
