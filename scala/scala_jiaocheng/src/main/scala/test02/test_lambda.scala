package test02

object test_lambda {
  def main(args:Array[String]):Unit = {
    val fun= (name:String) => {println(name)}
    fun("atguigu")

    def f(func:String=>Unit):Unit = {
      func("atguigu")
    }
    f(fun)
    f((name:String) => {println(name)})
    //省略参数类型声明(定义f时候已经声明)
    f((name)=>{println(name)})
    //一个参数可以省略括号
    f(name=>println(name))
    //如果参数只出现一次,则可以省略并用_表示
    f(println(_))

    f(println)
  }
}
