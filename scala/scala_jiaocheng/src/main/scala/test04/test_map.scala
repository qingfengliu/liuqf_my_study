package test04

object test_map {
  def main(args:Array[String]):Unit={
    val map1=Map("a"->13,"b"->25,"hello"->3)
    map1.foreach(println)
    map1.foreach( (kv:(String,Int))=>println(kv) )
    for (key<-map1.keys){
      println(s"$key----> ${map1.get(key)}")
    }

    //得到值
    println(map1.getOrElse("a","0"))
    println(map1("a"))
  }
}
