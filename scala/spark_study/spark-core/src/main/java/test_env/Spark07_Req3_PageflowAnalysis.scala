package test_env

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//需求3:页面单跳转换率
//用户在一次Session过程中访问的页面路径3,5,7,9,10,21那么页面3跳到页面5叫一次单跳,7-9也叫一次单跳
//页面A 访问量500W -> 页面B 访问量400W ->页面C 访问量300W ->页面D 访问量100W ->页面E 访问量10W
//页面A -> 页面B 单跳转化率:80%
//  页面B ->页面C 单跳转化率:75%
//  页面C->页面D 单跳转化率:30%
//  页面D->页面E 单跳转化率:10%
object Spark07_Req3_PageflowAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc = new SparkContext(sparkconf)
    //获取原始数据
    //Q1 存在大量的reduceByKey , 存在大量的shuffle操作
    //存在大量的reduceByKey聚合算子,Spark会提供优化,缓存
    //TODO TOP10热门品类
    val actionRDD = sc.textFile("D:\\packages\\user_visit_action.txt")
    actionRDD.cache()
    val actionDataRDD=actionRDD.map(
      action=>{
        val datas=action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )

      }
    )
    //1-2,2-3,3-4,4-5,5-6,6-7
    val ids=List[Long](1,2,3,4,5,6,7)
    val okflowIds =ids.zip(ids.tail)

    actionDataRDD.cache()
    //TODO 计算分母
    val pageidToCountMap:Map[Long,Long]=actionDataRDD.filter(
      action=>{
        //7不需要计算分母,所以加了个init
        ids.init.contains(action.page_id)
      }
    ).map(
      action=>{
        (action.page_id,1L)
      }
    ).reduceByKey(_+_).collect().toMap

    //TODO 计算分子
    //根据Session分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
    //分组后,根据访问时间进行排序(升序)
    val mvRDD= sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        //1.划窗函数? 2.zip
        val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

        //将不需要的页面跳转进行过滤
        pageFlowIds.filter(t=>{
          okflowIds.contains(t)
        }).map(t=>(t,1))
      }
    )
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    val dataRDD=flatRDD.reduceByKey(_+_)
    //TODO 计算单跳转换率
    dataRDD.foreach{
      case ((pageid1,pageid2),sum)=>{
        val lon: Long = pageidToCountMap.getOrElse(pageid1, 0)
        println(s"页面${pageid1}跳转到${pageid2}单跳转换率为:"+(sum.toDouble/lon))
      }
    }
    sc.stop()
  }
  case class UserVisitAction(
                            date:String, //用户点击行为的日期
                            user_id:Long,//用户点击的ID
                            session_id:String,//Session的ID
                            page_id:Long,//某个页面的ID
                            action_time:String,//动作的时间点
                            search_keyword:String,//用户搜索的关键词
                            click_category_id:Long,//某一商品品类的ID
                            click_product_id:Long,//某一商品的ID
                            order_category_ids:String,//一次订单中所有的品类ID集合
                            order_product_ids:String,//一次订单中所有商品ID集合
                            pay_category_ids:String,//一次支付中所有品类的ID集合
                            pay_product_ids:String,//一次支付中所有商品的ID集合
                            city_id:Long //城市ID
                            )

}
