package test01
import scala.io.Source
import java.io.{File, PipedWriter, PrintWriter}
object test_fileio {
  def main(args: Array[String]): Unit = {
    //读取文件
    Source.fromFile("D:\\书籍资料整理\\时光机器\\timemachine.txt").foreach(print)
    //写入文件 调用JAVA类
    val writer=new PrintWriter(new File("D:\\hello_scala.txt")) //注意这里pathname是ide添加的而实际是没有的
    writer.write("hello scala from java writer")
    writer.close()
  }
}
