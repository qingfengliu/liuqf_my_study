package test_env.framework.service

import org.apache.spark.rdd.RDD

import test_env.framework.common.TService
import test_env.framework.dao.WordCountDao

class WordCountService extends TService{
  private val wordCountDao=new WordCountDao()

  def dataAnalysis()={
    //(1)读取文件
    val lines=wordCountDao.readFile("datasets")
    val words=lines.flatMap(_.split(" "))

    val wordToOne=words.map(word=>(word,1))
    val wordGroup:RDD[(String,Iterable[(String,Int)])] =wordToOne.groupBy(t=>t._1)

    val wordToCount=wordGroup.map {
      case ( word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }
    wordToCount.collect().foreach(println)
    val array: Array[(String, Int)] = wordToCount.collect()
    array
  }
}
