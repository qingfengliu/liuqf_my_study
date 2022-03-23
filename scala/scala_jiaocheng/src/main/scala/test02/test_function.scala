package test02

object test_function {
  def main(args:Array[String]):Unit = {
    //可变参数、带名参数
    def f1(str: String*):Unit={
      println(str)
    }

    def f3(name:String = "atugugi"):Unit = {
      println(name)
    }

    def f4(name:String ="atugugi" ,age:Int):Unit = {
      println(s"${age}岁的${name}在尚硅谷学习")
    }
    f3()
    f3("school")
    f4(age=23,name="bob")
    f4(age=23)
//    f1("alice")
//    f1("aaa","bbb","ccc")
  }
}
