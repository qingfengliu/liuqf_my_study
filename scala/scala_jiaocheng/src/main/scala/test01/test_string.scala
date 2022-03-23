package test01

object test_string {
  def main(args: Array[String]): Unit ={
    val name="alice"
    val age=20
    println(age+"岁的"+name+"在商硅谷学习")
    printf("%d岁的%s在尚硅谷学习",age,name)
    //字符串模板
    println(s"${age}岁的${name}在尚硅谷学习")
    val num=2.345
    println(f"The num is ${num}%2.2f")
    val sql=
      s"""
         |select *
         |from
         |  student
         |where
         |  name=${name}
         |and
         |  age>${age}
         |""".stripMargin
    println(sql)
  }
}

