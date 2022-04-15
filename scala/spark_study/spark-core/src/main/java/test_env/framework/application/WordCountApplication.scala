package test_env.framework.application

import test_env.framework.common.TApplication
import test_env.framework.controller.WordCountController


//控制层
object WordCountApplication extends App with TApplication{
  //启动应用程序
  start(){
    val controller=new WordCountController()
    controller.dispatch()
  }
}
