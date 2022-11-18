import Utils.{DebugParams, Params, utils}
import F3KM.runF3_KM
import breeze.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
object driver {
  def main(args: Array[String]): Unit = {
    val options: Map[String, String] =  args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case Array(opt) => (opt -> "true")
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap
    // read in input
    val master: String = options.getOrElse("master", "local[3]")
    val dataset: String = options.getOrElse("trainFile", "datas/bank/bank.csv")
//    val colorset = options.getOrElse("color", "datas/bank/bank_color.csv")
    val numSamples: Int = options.getOrElse("numSamples", "4521").toInt
    val numSplits: Int = options.getOrElse("numSplits","3").toInt
    val chkptDir: String = options.getOrElse("chkptDir","chkpt");
    var chkptIter: Int = options.getOrElse("chkptIter","10").toInt

    // algorithm-specific inputs
    val model = "k-means"
    val numRounds: Int = options.getOrElse("numRounds", "50").toInt // number of iterations

    val k: Int = 4
    println("master:       " + master);           println("chkptDir:     " + chkptDir);
    println("dataset:      " + dataset);          println("chkptIter:    " + chkptIter);
    println("numSamples:   " + numSamples);       println("model:        " + model);
    println("numSplits:    " + numSplits);        println("numRounds:    " + numRounds);
    println("k:            " + k);
    // start spark context
    val conf = new SparkConf().setMaster(master)
      .setAppName("DFCDKM")
      .setJars(SparkContext.jarOfObject(this).toSeq)
    val sc = new SparkContext(conf)
    sc.setCheckpointDir(chkptDir)
    // read in data
    val data: RDD[Vector[Double]] = utils.loadData(sc, dataset, numSplits, numSamples).cache()
    val Fair = true
    val BlockSize = 1000
    val scaleU = 5e2
    val scaleRho = 1e4
    val delta = 0.2
    val lambda = 0   // violation in paper
    val LossUB =  78
    val params: Params = Params(numSamples, k, Fair,scaleU, scaleRho,delta,BlockSize)
    val debug: DebugParams = DebugParams(chkptIter,chkptDir,lambda,LossUB,numRounds)
    // run DisBCDKM or F3KM
    val result: (Iterable[SparseVector[Double]], ArrayBuffer[Double]) = runF3_KM(data: RDD[Vector[Double]], params: Params,numSplits,sc,debug)
    println("the end")
    sc.stop()
  }
}
