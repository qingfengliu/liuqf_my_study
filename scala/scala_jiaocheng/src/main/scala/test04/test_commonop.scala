package test04

object test_commonop {
  def main(args:Array[String]):Unit = {
    val list= List(1,3,5,7,2,89)
    val set= Set(23,34,423,75)
    val list2=List(3,7,2,45,8,19)
    println(list.length,set.size)
    for (elem<-list) println(elem)
    println(list.mkString(","))
    println(list.contains(3))
    println(list.head,list.tail,list.last,list.init)
    //反转
    println(list.reverse)
    //取前n和后n
    println(list.take(3),list.takeRight(3))
    //去掉
    println(list.drop(3),list.dropRight(3))
    //并集,交集
    println(list.concat(list2),list.intersect(list2))
    //差集diff,拉链zip生成二维list,滑动sliding

  }
}
