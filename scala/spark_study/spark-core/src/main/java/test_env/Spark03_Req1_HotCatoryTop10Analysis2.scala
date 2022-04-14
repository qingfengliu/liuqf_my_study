package test_env

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//日期 用户ID Session_id 页面ID 动作时间 搜索关键字
object Spark03_Req1_HotCatoryTop10Analysis2 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc = new SparkContext(sparkconf)
    //获取原始数据
    //Q1 存在大量的reduceByKey , 存在大量的shuffle操作
    //存在大量的reduceByKey聚合算子,Spark会提供优化,缓存
    //TODO TOP10热门品类
    val actionRDD = sc.textFile("D:\\packages\\user_visit_action.txt")
    actionRDD.cache()

    //将数据转换结构.
    //  点击  (品类ID,(1,0,0))
    //  下单  (品类ID,(0,1,0))
    //  支付  (品类ID,(0,0,1))
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
    //将相同的品类ID的数据进行分组聚合
    val analysisRDD=flatRDD.reduceByKey(
      (t1,t2) =>{
        (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
      }
    )
    val resultRDD=analysisRDD.sortBy(_._2,false).take(10)
    resultRDD.foreach(println)
  }
}
