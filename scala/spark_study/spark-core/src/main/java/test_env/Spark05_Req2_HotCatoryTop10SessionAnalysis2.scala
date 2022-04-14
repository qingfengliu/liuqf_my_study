package test_env

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//日期 用户ID Session_id 页面ID 动作时间 搜索关键字
object Spark05_Req2_HotCatoryTop10SessionAnalysis2 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc = new SparkContext(sparkconf)
    //获取原始数据
    //Q1 存在大量的reduceByKey , 存在大量的shuffle操作
    //存在大量的reduceByKey聚合算子,Spark会提供优化,缓存
    //TODO TOP10热门品类
    val actionRDD = sc.textFile("D:\\packages\\user_visit_action.txt")
    actionRDD.cache()
    val top10Ids: Array[String] = top10Caterory(actionRDD)
    //1.过滤原始数据,保留点击和前10品类ID
    val filterActiveRDD=actionRDD.filter(
      action=>{
        val datas=action.split("_")
        if (datas(6)!="-1"){
          top10Ids.contains(datas(6))
        }else{
          false
        }
      }
    )
    //2.根据品类ID和sessionid进行点击量的统计
    val reduceRDD=filterActiveRDD.map(
      action=>{
        val datas=action.split("_")
        ((datas(6),datas(2)),1)
      }
    ).reduceByKey(_+_)
    //进行结构转换((品类ID,sessionId),sum)=>(品类ID,(sessionId,sum))
    val mapRDD=reduceRDD.map{
      case ((cid,sid),sum)=>(cid,(sid,sum))
    }
    //相同品类进行分组.
    val groupRDD=mapRDD.groupByKey()
    //将分组后的数据点击量排序,取前10
    val resultRDD=groupRDD.mapValues(
      iter=>{
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    resultRDD.collect().foreach(println)
  }
  def top10Caterory(actionRDD:RDD[String])={

    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(action => {
      val datas = action.split("_")
      if (datas(6) != "-1") {
        //点击的场合
        List((datas(6), (1, 0, 0)))
      } else if (datas(8) != "null") {
        //下单的场合
        val ids = datas(8).split(",")
        ids.map(id => (id, (0, 1, 0)))
      } else if (datas(10) != "null") {
        //支付的场合
        val ids = datas(10).split(",")
        ids.map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }
    })

    val analysisRDD=flatRDD.reduceByKey(
      (t1,t2) =>{
        (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
      }
    )
    analysisRDD.sortBy(_._2,false).take(10).map(_._1)
  }
}
