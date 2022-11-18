package Utils
import breeze.linalg.{DenseMatrix, SparseVector, Vector}

case class Params(n: Int,
                  k:Int, Fair: Boolean,
                  scaleU: Double,
                  scaleRho: Double,
                  delta: Double,
                  BlockSize: Int)

case class DebugParams(chkptIter: Int,
                       chkptDir: String,
                       lambda: Double,
                       LossUB: Double,
                       numRound: Int
                )
// we construct a bag for fair update, all updates are completed in this bag.
case class FairBag(color: SparseVector[Double],
                   u: Vector[Double],
                   rho: Vector[Double],
                   theta: Vector[Double],
                   alpha: Double,
                   beta: Double,
                   FairLoss: DenseMatrix[Double],
                   PF: Array[Double]
                  )
