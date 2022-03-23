package test02

object test_forloop {
  def main(args:Array[String]):Unit = {
    for (i<-0 to 10){
      println(i +".hello word")
    }
    for (i<-0 until  10){
      println(i +".hello word")
    }
    for (i<- Array(12,345,53)){
      println(i)
    }
    for (i<-0 to 10 if i !=5){
      println(i +".hello word")
    }
    
    for (i<-0 to 10 by 2){
      println(i +".hello word")
    }
  }
}
