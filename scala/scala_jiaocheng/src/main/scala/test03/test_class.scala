package test03

import scala.beans.BeanProperty

class test_class {

}

object test03_class{
  def main(args:Array[String]):Unit={
    val student=new Student()
    println(student.age)
    println(student.sex)
    student.sex="female"
    println(student.sex)
  }
}
class Student{
  private var name:String ="alice"
  @BeanProperty
  var age:Int = _
  var sex:String = _
}
