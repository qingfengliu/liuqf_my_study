package test03

object test_trait {
  def main(args:Array[String]):Unit ={
    val student=new Student13
    student.sayHello()
    student.dating()
    student.play()
  }
}

//定义父类
class Person13(){
  val name:String = "Person"
  var age:Int = 18
  def sayHello()=println("hello from:" + name)
}

trait Young{
  //定义抽象和非抽象属性
  var age:Int
  val name:String="Young"
  //定义抽象和非抽象方法

  def play():Unit={
    println("young people is playing")
  }

  def dating():Unit
}

class Student13 extends Person13 with Young {
  //重写冲突的属性
  override val name:String = "student"
  override def dating(): Unit = {
    println(s"student $name is dating")
  }
  def study()=println(s"student $name is studying")

  override def sayHello():Unit={
    super.sayHello()
    println(s"hello from:student $name")
  }

}