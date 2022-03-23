package test04
import scala.collection.mutable.ArrayBuffer

object test_imarray {
  def main(args:Array[String]):Unit={
    var arr:Array[Int]=new Array[Int](5)
    val arr2=Array(12,37,42,58,97)
    println(arr(0))
    arr(0)=1
  }
}

object test_array_buff {
  def main(args:Array[String]):Unit={
    val arr1=new ArrayBuffer[Int]()
    val arr2=ArrayBuffer(23,57,92)
    println(arr2(0))
  }
}

object test_mularray {
  def main(args:Array[String]):Unit={
    val array=Array.ofDim[Int](2,3)
    array(0)(2)=19

  }
}