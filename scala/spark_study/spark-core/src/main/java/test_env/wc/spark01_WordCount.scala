package test_env.wc
import org.apache.spark.{SparkContext,SparkConf}
//执行的前提必须配置windows spark环境也就是不需要虚拟机了
object spark01_WordCount {
  def main(args: Array[String]): Unit = {
//    println(scala.util.Properties.envOrElse("HADOOP_HOME", "empty"))
//    println(scala.util.Properties.envOrElse("PATH", "empty"))
        //Spark框架
    //应用程序
    //1.建立与spark的连接
    //JDBC:Connection
    val master = "local"
    val sparkConf=new SparkConf().setMaster(master).setAppName("WordCount")
    val sc=new SparkContext(sparkConf)

    //(1)读取文件
    val lines =sc.textFile("datasets")
    val words=lines.flatMap(_.split(" "))
    val wordGroup =words.groupBy(word=>word)   //此处是一个lambda表达式
    //group by调用后应该生成了map数据,然后可以用下边的方法解开
    val wordToCount=wordGroup.map {
      case (word, list) => {
        {
          (word, list.size)
        }
      }
    }
    val array=wordToCount.collect()
    array.foreach(println)

//    (2)将一行数据进行拆分,形成一个一个的单词(分词)
//    (3)将数据分组
//
//
//    2.执行业务操作
//    3.关闭连接
    sc.stop()
  }
}
