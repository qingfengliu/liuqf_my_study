package src.main.scala.test04

object test_machobject {
  def main(args:Array[String]):Unit={
    val student=new Student("alice",18)
    val result= student match {
      case Student("alice",18)=>"Alice,18"
      case _=>"Else"
    }
    println(result)
  }
}

class Student(val name:String,val age:Int)

object Student{
  def apply(name:String,age:Int):Student = new Student(name,age)
  //必须定义一个unapply方法用来对对象属性进行拆解
  def unapply(student: Student):Option[(String,Int)]={
    if (student==null){
      None
    } else {
      Some((student.name,student.age))
    }
  }
}