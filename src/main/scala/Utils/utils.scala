package Utils
import breeze.numerics._
import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer


object utils {
  def loadData(sc: SparkContext, filename: String, numSplits: Int, numSamples: Int): RDD[Vector[Double]] = {
    // read in text file
    val data: RDD[String] = sc.textFile(filename, numSplits).coalesce(numSplits)
    data.mapPartitionsWithIndex{case(partition, lines) => lines.map(line=>Vector(line.split(",").map(_.toDouble)))}
  }
  def loadColor(sc: SparkContext, ColorName: String, numSplits: Int, numSamples: Int)={
    // read color from text file
    val color: RDD[String] = sc.textFile(ColorName, numSplits).coalesce(numSplits)
    color.mapPartitionsWithIndex{(partition, lines) => lines.map(line=>line.split(","))}.map(x=>Vector(x.map(_.toDouble)))
  }

  def RandIntVec(n: Int, k: Int):Array[Int]={
    // construct an Array for initial label
    val InitialLabelArr: ArrayBuffer[Int] = ArrayBuffer[Int]()
    for (i<-1 to n) {
      val clusterIdx: Int = scala.util.Random.nextInt(k)
      InitialLabelArr += clusterIdx
    }
    InitialLabelArr.toArray
  }
  def RandU(scale: Double, k: Int):Array[Double]={
    // construct an initial u.
    val InitU: ArrayBuffer[Double] = ArrayBuffer[Double]()
    for (i<-1 to k) {
      val clusterIdx: Double = scale * scala.util.Random.nextDouble()
      InitU += clusterIdx
    }
    InitU.toArray
  }
  def RandTheta(alpha: Double, beta: Double, k: Int):Array[Double]={
    // construct an initial theta.
    val InitTheta: ArrayBuffer[Double] = ArrayBuffer[Double]()
    for (i<-1 to k) {
//      val clusterIdx: Double = min(alpha,max(scala.util.Random.nextDouble(),beta))
      val clusterIdx: Double = min(alpha,max(10.0,beta))
      InitTheta += clusterIdx
    }
    InitTheta.toArray
  }
  def ComputeLoss(F:Iterable[SparseVector[Double]],
                  data:RDD[Vector[Double]],
                  k: Int): Double ={
    val clusterIds: Iterable[Array[Int]] = F.map(y => y.array.index)
    val ClusterWithIDs: Iterable[(Array[Int], Int)] = clusterIds.zipWithIndex
    val DimCenter: Array[Double] = new Array[Double](k)
    var ClusterLoss: Array[Double] = new Array[Double](k)
    var LossSum: RDD[Double] = data.map { x =>
      for ((ids, clusterId) <- ClusterWithIDs) {
        DimCenter(clusterId) = x(ids.toList).reduce(_+_)/ids.length
        ClusterLoss(clusterId) = x(ids.toList).map(y => math.pow(y - DimCenter(clusterId), 2)).reduce(_+_)
      }
      val Loss: Double = ClusterLoss.sum
      Loss
    }
    math.sqrt(LossSum.reduce(_+_))
  }
  def StopCriteria(Fair: Boolean,
                   Loss: ArrayBuffer[Double],
                   i:Int,
                   Fairbag: Array[Array[FairBag]],
                   F: Iterable[SparseVector[Double]],
                   lambda: Double,
                   LossUB: Double
                  ): Boolean ={
    val Farr: Array[SparseVector[Double]] = F.toArray
    if (Fair) {
      val StopSet: Array[Boolean] = Fairbag.flatMap(_.map(x => {
        val color: SparseVector[Double] = x.color
        val alpha: Double = x.alpha
        val beta: Double = x.beta
        val TorF: Array[Boolean] = Farr.map(x => {
          if ((beta <= (x.dot(color) + lambda) / sum(x)) && (alpha >= ((x.dot(color) + lambda) / sum(x)))) true else false
        })
        TorF.reduce(_ && _)
      }))
      val FairStop: Boolean = StopSet.reduce(_ && _)
      val stop:Boolean = if ((Loss(i)<=LossUB) && FairStop) true else false
      stop
    }
    else {if (Loss(i)-Loss(i-1)==0.00000) true else false}
  }

}
