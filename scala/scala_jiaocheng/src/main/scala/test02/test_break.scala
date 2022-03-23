package test02

import scala.util.control.Breaks

object test_break {
  def main(args:Array[String]): Unit = {
    try {
      for (i<-0 to 5){
         if (i==3) {
           throw new RuntimeException
         }
      }
    }catch {
      case e: Exception =>
    }
    println("这是循环外的代码")

    Breaks.breakable(
      for (i<-0 to 5){
        if (i==3) {
          Breaks.break()
        }
      }
    )
  }


}
