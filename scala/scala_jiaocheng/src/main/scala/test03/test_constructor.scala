package test03

object test_constructor {
  def main(args:Array[String]):Unit = {
    val student1=new Student1()
    val student2=new Student1("Alice")
    val student3=new Student1("bob",25 )
  }
}

class Student1{
  var name:String = _
  var age:Int = _

  println("1.主构造方法被调用")

  def this(name:String){
    this()
    println("2.辅助构造器方法一被调用")
    this.name=name
    println(s"name:${name} age:${age}")
  }

  def this(name:String,age:Int){
    this(name)
    println("3.辅助构造器方法二被调用")
    this.age=age
    println(s"name:${name} age:${age}")
  }
}
