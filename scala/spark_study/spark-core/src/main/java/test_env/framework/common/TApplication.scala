package test_env.framework.common
import org.apache.spark.{SparkConf, SparkContext}
import test_env.framework.util.EnvUtil


trait TApplication {
  def start(master: String="local[*]",app:String="Application")(op: => Unit):Unit={
    val sparkConf=new SparkConf().setMaster(master).setAppName(app)
    val sc=new SparkContext(sparkConf)
    EnvUtil.put(sc)
    try {
      op
    }catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }

}
