package test_env.wc
import org.apache.spark.{SparkContext,SparkConf}
//执行的前提必须配置windows spark环境也就是不需要虚拟机了
object spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //Spark框架
    //应用程序
    //1.建立与spark的连接
    //JDBC:Connection
    val master = "spark://192.168.42.128:7077"

//    val remote_file = "hdfs://spark1:9000/user/spark/data/spark.txt"
    val sparkConf=new SparkConf().setMaster(master).setAppName("WordCount")
    val sc=new SparkContext(sparkConf)

    //2.执行业务操作
    //3.关闭连接
    sc.stop()
  }
}
