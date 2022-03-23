package test01

class Student(name:String,age:Int) {
  def printinfo():Unit = {
    println(name+" "+age+" "+Student.school)
  }
}

//引入伴生对象
object Student{
  val school:String="atguigu"
  def main(args: Array[String]): Unit={
    val alice=new Student(name = "alice",age = 20)
    val bob=new Student(name = "bob",age = 23)
    alice.printinfo()
    bob.printinfo()
  }
}
