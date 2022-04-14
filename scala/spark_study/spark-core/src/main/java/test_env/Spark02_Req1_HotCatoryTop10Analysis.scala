package test_env

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//日期 用户ID Session_id 页面ID 动作时间 搜索关键字
object Spark02_Req1_HotCatoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc = new SparkContext(sparkconf)
    //获取原始数据
    //TODO TOP10热门品类
    val file_rdd = sc.textFile("D:\\packages\\user_visit_action.txt")

    //统计品类的点击数量
    val clickActionRDD = file_rdd.filter(action => {
      val datas = action.split("_")
      datas(6) != "-1"
    })

    val clickCountRDD = clickActionRDD.map(action => {
      val datas = action.split("_")
      (datas(6), 1)
    }).reduceByKey(_ + _)

    //统计品类的下单数量
    val orderActionRDD = file_rdd.filter(action => {
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

    val payActionRDD = file_rdd.filter(action => {
      val datas = action.split("_")
      datas(10) != "null"
    })
    //orderid=> 1,2,3
    // (1,1) (2,1) (3,1)
    val payCountRDD=orderActionRDD.flatMap(
      action=>{
        val datas = action.split("_")
        val cid=datas(10)
        val cids=cid.split(",")
        cids.map(id=>(id,1))
      }
    ).reduceByKey(_+_)

    //5.将品类进行排序,并且取前10
    //首先将数据整理成 (品类ID,(点击数量,下单数量,支付数量))
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    // 把iterable展开
    val analysisRDD=cogroupRDD.mapValues{
      case (clickIter,orderIter,payIter) => {
        var clickCnt=0
        val iter1=clickIter.iterator
        if (iter1.hasNext){
          clickCnt=iter1.next()
        }
        var orderCnt=0
        val iter2=orderIter.iterator
        if (iter2.hasNext){
          orderCnt=iter2.next()
        }
        var payCnt=0
        val iter3=payIter.iterator
        if (iter3.hasNext){
          payCnt=iter3.next()
        }
        (clickCnt,orderCnt,payCnt)
      }
    }
    val resultRDD=analysisRDD.sortBy(_._2,false).take(10)
    resultRDD.foreach(println)
  }
}
