package test02

object test_lazy {
  def main(args:Array[String]):Unit = {
    lazy val result=sum(13,47)

    println("1.函数调用")
    println("2. result = "+result)
  }
  def sum(a:Int,b:Int):Int = {
    println("3.调用sum")
    return a+b
  }
}
