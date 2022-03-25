package src.main.scala.test04

object test_caseclass {
  def main(args:Array[String]):Unit={
    val student=Student2("alice",18)
    val result= student match {
      case Student2("alice",18)=>"Alice,18"
      case _=>"Else"
    }
    println(result)
  }
}

case class Student2(name:String,age:Int)

