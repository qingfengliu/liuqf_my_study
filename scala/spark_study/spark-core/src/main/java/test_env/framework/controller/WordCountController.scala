package test_env.framework.controller
import test_env.framework.common.TController
import test_env.framework.service.WordCountService

class WordCountController extends TController{
  private val wordCountService=new WordCountService()


  def dispatch()={
    val array=wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
