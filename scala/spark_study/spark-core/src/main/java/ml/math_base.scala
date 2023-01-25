package ml
import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector, SparseVector}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.ml.linalg.{Matrix,Matrices, Vector, Vectors}

object math_base {
  def main(args: Array[String]): Unit = {
    //普通向量
    val dVectorOne: Vector = Vectors.dense(1.0,0.0,2.0)
    //稀疏向量

    val sVectorOne: Vector = Vectors.sparse(4,  Array(0, 2,3),  Array(1.0, 2.0, 3.0))
    val sVectorTwo: Vector = Vectors.sparse(4,  Seq((0, 1.0), (2, 2.0), (3, 3.0)))

    println("dVector:"+dVectorOne)
    println("sVectorOne:" + sVectorOne)
    println("sVectorTwo:" + sVectorTwo)

    //spark底层是 breeze 所以书里用这个包向量计算较多 实际ml的dense和sparse也是能用的
    val a = DenseVector(0.56390, 0.36231, 0.14601, 0.60294, 0.14535)
    val b = DenseVector(0.15951, 0.83671, 0.56002, 0.57797, 0.54450)
    println(a.t * b)
    println(a dot b)
    println("-----------------------------------------------")
    val sva = SparseVector(0.56390,0.36231,0.14601,0.60294,0.14535)
    val svb = SparseVector(0.15951,0.83671,0.56002,0.57797,0.54450)
    println(sva.t * svb)
    println(sva dot svb)
    println("-----------------------------------------------")

    //矩阵
    //密集矩阵 或 CSC（列压缩稀疏）矩阵
    //密集矩阵
    val a2 = DenseMatrix((1,2),(3,4))
    val m = DenseMatrix.zeros[Int](5,5)
    // 各列可以 Dense Vector 方式访问，而行则可作为 Dense Matrix 来访问
    println("密集矩阵:")
    println(a2)
    println( "m.rows :" + m.rows + " m.cols : "  + m.cols)
    println("First Column of m : \n" + m(::,1))

    m(4,::) := DenseVector(5,5,5,5,5).t
    println(m)

    //CSC  。建立了一个10*10的稀疏矩阵,矩阵在(3,4)位置上有一个1值
    val builder = new CSCMatrix.Builder[Double](rows=10, cols=10)
    builder.add(3,4, 1.0)

    val myMatrix = builder.result()
    println("CSC矩阵:")
    println(myMatrix)

    println("-----------------------------------------------")
    //spark里的矩阵
    println("spark中的矩阵")
    val dMatrix: Matrix = Matrices.dense(2, 2, Array(1.0, 2.0, 3.0, 4.0))
    println("dMatrix: \n" + dMatrix)
    val sMatrixOne: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3),
      Array(0, 2, 1), Array(5, 6, 7))
    println("sMatrixOne: \n" + sMatrixOne)
    val sMatrixTwo: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3),
      Array(0, 1, 2), Array(5, 6, 7))
    println("sMatrixTwo: \n" + sMatrixTwo)

    println("-----------------------------------------------")
    //spark里的矩阵分布式矩阵, 这里会引发冲突
    println("spark中分布式矩阵")
    //行优先矩阵
    println("行优先模式存储 在行优先的矩阵里，每一行的相邻元素在内存中也是相邻存储的。")
    val spConfig = (new SparkConf).setMaster("local")
      .setAppName("SparkApp")
    val sc = new SparkContext(spConfig)

    val denseData = Seq(
      org.apache.spark.mllib.linalg.Vectors.dense(0.0, 1.0, 2.1),
      org.apache.spark.mllib.linalg.Vectors.dense(3.0, 2.0, 4.0),
      org.apache.spark.mllib.linalg.Vectors.dense(5.0, 7.0, 8.0),
      org.apache.spark.mllib.linalg.Vectors.dense(9.0, 0.0, 1.1)
    )
    val sparseData = Seq(
      org.apache.spark.mllib.linalg.Vectors.sparse(3, Seq((1, 1.0), (2, 2.1))),
      org.apache.spark.mllib.linalg.Vectors.sparse(3, Seq((0, 3.0), (1, 2.0), (2, 4.0))),
      org.apache.spark.mllib.linalg.Vectors.sparse(3, Seq((0, 5.0), (1, 7.0), (2, 8.0))),
      org.apache.spark.mllib.linalg.Vectors.sparse(3, Seq((0, 9.0), (2, 1.0)))
    )

    val denseMat = new RowMatrix(sc.parallelize(denseData, 2))
    val sparseMat = new RowMatrix(sc.parallelize(sparseData, 2))

    println("Dense Matrix - Num of Rows :" + denseMat.numRows())
    println("Dense Matrix - Num of Cols:" + denseMat.numCols())
    println("Sparse Matrix - Num of Rows :" + sparseMat.numRows())
    println("Sparse Matrix - Num of Cols:" + sparseMat.numCols())
    sc.stop()

    //除了 RowMatrix 还有 IndexedRowMatrix CoordinateMatrix
  }
}
