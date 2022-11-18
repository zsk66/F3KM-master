import breeze.linalg._
import breeze.numerics._
import org.apache.spark.rdd.RDD
import Utils._
import Utils.utils.{ComputeLoss, RandIntVec, RandTheta, RandU, StopCriteria, loadData}
import org.apache.spark.SparkContext
import breeze.linalg._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.CollectionAccumulator
import Array._
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

object F3KM {
  def runF3_KM(data:RDD[Vector[Double]],
               params:Params,
               numSplits:Int,
               sc: SparkContext,
               debug: DebugParams,
               ): (Iterable[SparseVector[Double]], ArrayBuffer[Double])={
    val Fair: Boolean = params.Fair
    val alg: String = if (Fair) "F3KM" else "Dis-BCDKM"
    println("\nRunning " + alg +" on "+params.n+" data examples, distributed over "+numSplits+" workers")
//    val LabelSource: BufferedSource = Source.fromFile("datas/bank/Initlabel.txt", "UTF-8")
//    var Label: Array[Int] = LabelSource.getLines().toArray.map(_.toInt-1)
    val ColorSource: BufferedSource  = Source.fromFile("datas/bank/bank_color.txt", "UTF-8")
    val color: Array[Array[Int]] = ColorSource.getLines().map(_.split(',').map(_.toInt)).toArray
    val SortedColor: Array[Map[Int, Array[(Int, Int)]]] = color.map(x => x.zipWithIndex.groupBy(_._1).toList.sortBy(_._1).toMap)
    var dataArr: RDD[Array[Vector[Double]]] = data.mapPartitions(x => Iterator(x.toArray))
    var Label: Array[Int] = RandIntVec(params.n, params.k)
    var F: Iterable[SparseVector[Double]] = Label.zipWithIndex.groupBy(_._1).toList.sortBy(_._1).map(x=>new SparseVector[Double](x._2.map(_._2),x._2.map(_._2).map(z=>1.0),params.n)).toIterable
    val InitLoss: Double = ComputeLoss(F: Iterable[SparseVector[Double]], data: RDD[Vector[Double]], params.k)
    println(InitLoss)
    var Loss: ArrayBuffer[Double] = ArrayBuffer(InitLoss)
    val BlockNum: Int = math.ceil(params.n.toDouble/params.BlockSize.toDouble).toInt
    var XFVar: RDD[Vector[Double]] = data.map(x=>Vector(new Array[Double](params.k)))
    var XF: RDD[Array[Vector[Double]]] = XFVar.mapPartitions(x=>Iterator(x.toArray))

    val Norm1F: Array[Double] = F.map(f => sum(f)).toArray
    var Fairbag: Array[Array[FairBag]] = SortedColor.map(x => {
      val l: Int = x.values.size
      x.values.map(y => {
        val color: SparseVector[Double] = new SparseVector[Double](y.map(_._2), y.map(_._2).map(_ => 1.0), params.n)
        var Uk: Vector[Double] = Vector(new Array[Double](params.k).map(_=>params.scaleU))
        var rho_k: Vector[Double] = Vector(new Array[Double](params.k).map(_=>params.scaleRho))
        val FairLoss: DenseMatrix[Double] = DenseMatrix.zeros[Double](params.n,params.k)
        val alpha: Double = sum(color)/(params.n*(1-params.delta))
        val beta: Double = sum(color)*(1-params.delta)/params.n
        val Theta_k: Vector[Double] = Vector(RandTheta(alpha, beta, params.k))
        val PF: Array[Double] = F.map(x => x.dot(color)).toArray
        val bag: FairBag = FairBag(color, Uk, rho_k, Theta_k, alpha, beta,FairLoss,PF)
        bag
      })
    }.toArray)
    val colors: Array[SparseVector[Double]] = Fairbag.flatMap(x => x.map(x => x.color))
    var ColorRDD: RDD[Array[FairBag]] = sc.parallelize(Fairbag, numSplits)
    var ToUpdate: Array[((Int, Int), Int)] = Label.zipWithIndex.zip(Label)
    var stop = false

    val start = System.currentTimeMillis
    for (i<-1 to debug.numRound if !stop){
      for (blockIdx <- 1 to BlockNum){
        // Update F
        val zipData: RDD[(Array[Vector[Double]], Array[Vector[Double]])] = XF.zip(dataArr)
        val idxLow: Int = params.BlockSize*(blockIdx-1)
        val idxUp: Int = if (blockIdx*params.BlockSize>params.n){params.n} else{params.BlockSize*blockIdx}
        var Phi: DenseMatrix[Double] = new DenseMatrix[Double](idxUp-idxLow,params.k)
        val result: RDD[Tuple2[DenseMatrix[Double],Array[Vector[Double]]]] = zipData.mapPartitions((partitionUpdate(_, F,Label, params.n,params.k,
                  params.BlockSize,blockIdx,i,idxLow,idxUp,ToUpdate,Norm1F)), preservesPartitioning = true).persist()
        val LocalPhi: RDD[DenseMatrix[Double]] = result.map(_._1).persist()
        XF = result.map(_._2)
        if (Fair){
          ColorRDD = ColorRDD.mapPartitions(iter => iter.map(FairUpdate(_, F,params.n,params.k,
            Label,idxLow,idxUp,params.BlockSize,blockIdx,ToUpdate,i))).persist()
          Phi = (LocalPhi.reduce(_+_)+ColorRDD.map(_.map(_.FairLoss).reduce(_+_)).reduce(_+_)).t
        } else {Phi = LocalPhi.reduce(_+_).t}
        val LabelUpdate: Array[Int] = Phi.toArray.grouped(Phi.rows).toArray.map(y => argmin(y))
        ToUpdate = Label.zipWithIndex.slice(idxLow, idxUp).zip(LabelUpdate).filterNot { case (a, b) => a._1 == b }
        for (j <-idxLow until idxUp){
          Label.update(j,LabelUpdate(j-params.BlockSize*(blockIdx-1)))
        }
        F= Label.zipWithIndex.groupBy(_._1).toList.sortBy(_._1).map(x=>new SparseVector[Double](x._2.map(_._2),x._2.map(_._2).map(z=>1.0),params.n)).toIterable
        val updateNum: Int = ToUpdate.length
        for (j <- 0 until updateNum){
          val q: Int = ToUpdate(j)._2
          val p: Int = ToUpdate(j)._1._1
          Norm1F(q) =  Norm1F(q) + 1
          Norm1F(p) =  Norm1F(p) - 1
        }
        if(i % debug.chkptIter == 0){
          zipData.checkpoint()
        }
      }
      Loss += ComputeLoss(F: Iterable[SparseVector[Double]], data: RDD[Vector[Double]], params.k)

      // Update theta and u
      if (Fair){
        ColorRDD = ColorRDD.mapPartitions(iter=>iter.map(ParamsUpdate(_,F,params.k)))
      }
      println("Iteration: " + i +"----"+"Loss: "+Loss(i))
//      println("Loss: "+Loss(i))

      stop = StopCriteria(Fair, Loss, i, Fairbag, F, debug.lambda, debug.LossUB)

    }
    val end = System.currentTimeMillis

    printf("Run Time = %f[s]\n", (end - start) / 1000.0)
    return (F,Loss)

  }
  def partitionUpdate(zipData: Iterator[(Array[Vector[Double]],Array[Vector[Double]])],
                      F: Iterable[SparseVector[Double]],
                      Label: Array[Int],
                      n: Int,
                      k: Int,
                      BlockSize: Int,
                      blockIdx: Int,
                      i: Int,
                      idxLow: Int,
                      idxUp: Int,
                      ToUpdate: Array[((Int, Int), Int)],
                      Norm1F: Array[Double]
                     ):Iterator[Tuple2[DenseMatrix[Double],Array[Vector[Double]]]]={

    val zipPair: (Array[Vector[Double]], Array[Vector[Double]]) = zipData.next()
    val LocalData: Array[Vector[Double]] = zipPair._2
    val XF: Array[Vector[Double]] = if(i==1){F.map(f => Vector(LocalData.map(x => x.dot(f)))).toArray}else{
      val updateNum: Int = ToUpdate.length
      val XFTmp: Array[Vector[Double]] = zipPair._1
      for (j <- 0 until updateNum){
        val dataID: Int = ToUpdate(j)._1._2
        val x_i: Vector[Double] = Vector(LocalData.map(x => x(dataID)))
        val q: Int = ToUpdate(j)._2
        val p: Int = ToUpdate(j)._1._1
        XFTmp(q) = XFTmp(q) + x_i
        XFTmp(p) = XFTmp(p) - x_i
      }
      XFTmp
    }
    val result: Tuple2[DenseMatrix[Double],Array[Vector[Double]]] = LocalCompute(LocalData, XF,F, Label, n, k,BlockSize,blockIdx,idxLow,idxUp,Norm1F,ToUpdate,i)

    Iterator(result)
  }
  def LocalCompute(LocalData: Array[Vector[Double]],
                   XF: Array[Vector[Double]],
                   F: Iterable[SparseVector[Double]],
                   Label: Array[Int],
                   n: Int,
                   k: Int,
                   BlockSize: Int,
                   blockIdx: Int,
                   idxLow: Int,
                   idxUp: Int,
                   Norm1F: Array[Double],
                   ToUpdate: Array[((Int, Int), Int)],
                   i: Int
                  ):Tuple2[DenseMatrix[Double],Array[Vector[Double]]]={

    val block: Int = idxUp - idxLow
    val phi: DenseMatrix[Double] = DenseMatrix.zeros[Double](block,k)
    val V1: DenseMatrix[Double] = DenseMatrix.zeros[Double](block,k)
    val V2: DenseMatrix[Double] = DenseMatrix.zeros[Double](block,k)
    val FXXF: Array[Double] = XF.map(x => x.dot(x))
    for (i <- 0 until block){
      val dataID: Int = i+BlockSize*(blockIdx-1)
      val x_i: Vector[Double] = Vector(LocalData.map(x => x(dataID)))
      for (j <- 0 until k){
        val index: Int = Label(dataID)
        if (index==j){
          V1(i,j)=FXXF(j)-2.0* XF(j).dot(x_i)
          phi(i,j)=(V1(i,j)/(Norm1F(j)-1.0))-(FXXF(j)/Norm1F(j))
        }
        else{
          V2(i,j)=FXXF(j)+2.0*XF(j).dot(x_i)
          phi(i,j)=(FXXF(j)/Norm1F(j)) -(V2(i,j)/(Norm1F(j)+1.0))
        }
      }
    }
    (phi,XF)
  }
  def FairUpdate(PartitionFairBag: Array[FairBag],
                 F: Iterable[SparseVector[Double]],
                 n: Int,
                 k: Int,
                 Label: Array[Int],
                 idxLow: Int,
                 idxUp: Int,
                 BlockSize: Int,
                 blockIdx: Int,
                 ToUpdate: Array[((Int, Int), Int)],
                 i: Int
                ): Array[FairBag] ={
    val l: Int = PartitionFairBag.length
    val block: Int = idxUp - idxLow
    val NewFairBag: Array[FairBag] = PartitionFairBag.map(x => {
      var FairLoss: DenseMatrix[Double] = DenseMatrix.zeros[Double](block, k)
      var Lagrange: DenseMatrix[Double] = DenseMatrix.zeros[Double](block, k)
      var AugLagrange: DenseMatrix[Double] = DenseMatrix.zeros[Double](block, k)
      val color: SparseVector[Double] = x.color
      val alpha: Double = x.alpha
      val beta: Double = x.beta
      val u_k: Vector[Double] = x.u
      val rho_k: Vector[Double] = x.rho
      val theta_k: Vector[Double] = x.theta
      val Norm1F: Array[Double] = F.map(f => sum(f)).toArray
      val Arr_F: Array[SparseVector[Double]] = F.toArray
      val PF: Array[Double] = F.map(x => x.dot(color)).toArray
      val PFdivNorm: Array[Double] = F.map(x => x.dot(color) / sum(x)).toArray
      for (i <- 0 until block) {
        val dataID: Int = i+BlockSize*(blockIdx-1)
        val delta_k: SparseVector[Double] = SparseVector(new Array[Double](n))
        delta_k(dataID) = 1
        val index: Int = Label(dataID)
        for (j <- 0 until k) {
          if (index == j) {
            val PF_div_F1: Double = PF(j)/Norm1F(j)
            val PF_div_F2: Double = (PF(j)-delta_k.dot(color))/(Norm1F(j)-1)
            val PF_sub: Double = PF_div_F1 - PF_div_F2
            val PF_sum: Double = PF_div_F1 + PF_div_F2
            Lagrange(i, j) = -u_k(j)*PF_sub
            AugLagrange(i, j) = (rho_k(j)/2)*(PF_sub*(PF_sum-2*theta_k(j)))
            FairLoss(i, j) = Lagrange(i, j) + AugLagrange(i, j)
          } else {
            val PF_div_F1: Double = PF(j)/Norm1F(j)
            val PF_div_F2: Double = (PF(j)+delta_k.dot(color))/(Norm1F(j)+1)
            val PF_sub: Double = PF_div_F2 - PF_div_F1
            val PF_sum: Double = PF_div_F2 + PF_div_F1
            Lagrange(i, j) = -u_k(j)*PF_sub
            AugLagrange(i, j) = (rho_k(j)/2)*(PF_sub*(PF_sum-2*theta_k(j)))
            FairLoss(i, j) = Lagrange(i, j) + AugLagrange(i, j)
          }
        }
      }

      val Newfairbag: FairBag = FairBag(color,u_k,rho_k,theta_k,alpha,beta,FairLoss,PF)
      Newfairbag
    })
    NewFairBag
  }
  def ParamsUpdate(PartitionFairBag:Array[FairBag],
                   F: Iterable[SparseVector[Double]],
                   k: Int,
                   ): Array[FairBag]={
    val NewFairBag: Array[FairBag] = PartitionFairBag.map(x => {
      val color: SparseVector[Double] = x.color
      val alpha: Double = x.alpha
      val beta: Double = x.beta
      val u_k: Vector[Double] = x.u
      val rho_k: Vector[Double] = x.rho
      val theta_k: Vector[Double] = x.theta
      val PF: Array[Double] = x.PF
      val PFdivNorm: Array[Double] = F.map(x => x.dot(color) / sum(x)).toArray
      for (j<-0 until k){
        theta_k(j) = PFdivNorm(j)-u_k(j)/rho_k(j)
        theta_k(j) = min(alpha,max(theta_k(j),beta))
        u_k(j) = u_k(j) + rho_k(j)*(theta_k(j)-PFdivNorm(j))
      }
      val Newfairbag: FairBag = FairBag(color,u_k,rho_k,theta_k,alpha,beta,x.FairLoss,PF)
      Newfairbag
    })
    NewFairBag
  }
}
