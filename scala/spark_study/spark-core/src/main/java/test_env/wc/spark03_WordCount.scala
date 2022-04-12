package test_env.wc
import org.apache.spark.{SparkConf, SparkContext}
//执行的前提必须配置windows spark环境也就是不需要虚拟机了
object spark03_WordCount {
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

    //group by调用后应该生成了map数据,然后可以用下边的方法解开

    val wordToOne=words.map(
      word=>(word,1)
    )

    //相同的key数据可以对value进行reduce聚合
    val wordToCount=wordToOne.reduceByKey(_+_)  //wordToOne.reduceByKey((x,y)=>{x+y})

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

