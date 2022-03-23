package test02
import scala.io.StdIn
object test_ifelse {
  def main(args:Array[String]): Unit = {
    println("请输入您的年龄:")
    val age: Int=StdIn.readInt()
    if (age>=18){
      println("成年")
    }else{
      println("未成年")
    }
    println("==========================")
    if (age<=6){
      println("童年")
    }else if(age<18){
      println("青少年")
    }else if(age<35){
      println("青年")
    }else if(age<60){
      println("中年")
    }else{
      println("老年")
    }
    println("==========================")
    val result:String= if (age<=6){
      println("童年")
      "童年"
    }else if(age<18){
      println("青少年")
      "青少年"
    }else if(age<35){
      println("青年")
      "青年"
    }else if(age<60){
      println("中年")
      "中年"
    }else{
      println("老年")
      "老年"
    }
  }
}
