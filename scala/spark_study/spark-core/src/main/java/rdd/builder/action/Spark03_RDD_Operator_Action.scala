package rdd.builder.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkconf)

    val rdd=sc.makeRDD(List(1,2,3,4))

    val user = new User()

    //Task not serializable
    //RDD算子中的函数是会包含闭包操作的.就会有检测功能?

    rdd.foreach(
      num=>{
        println("age = " + (user.age+num))
      }
    )

    sc.stop()
  }

  //这个类需要序列化才会被executor使用
  //class User extends Serializable{
  //或者使用样例类,在样例类编译时,会自动混入序列化

  case class User(){
    var age:Int=30
  }
}
