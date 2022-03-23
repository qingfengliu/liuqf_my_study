package test04

import scala.collection.mutable.ListBuffer

object test_list {
  def main(args:Array[String]):Unit={
    var list1=List(23,19,87)
    list1.foreach(println)
    list1=list1.::(53)
    println(list1)

    val list2=Nil.::(13)
    val list3=13 :: 14::4::5::Nil
    val list4=list1:::list2
  }
}

object test_listbuffer {
  def main(args:Array[String]):Unit={
    val list1=ListBuffer(23,19,87)
    31+=:96+=:list1+=25+=11
  }
}