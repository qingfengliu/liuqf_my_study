package test01

object test_var {
  def main(args: Array[String]): Unit ={
    var a:Int = 10
    var a1 =20
    val b1=24
    //不能把string付给Int类型变量  强类型
    //不允许没有初值的变量
    //val如果指向类,val不可更改指向的类实体,但是可以更改内部的变量
    var alice=new Student(name="alice",age=20)

  }
}
