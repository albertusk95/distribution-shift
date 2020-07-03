package stats.distributions

import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest
import org.apache.spark.sql.DataFrame
import stats.configs.ColumnConfig
import stats.constants.DistributionConstants

object DistributionEvaluation {
  def evaluate(
    sampleOneDf: DataFrame,
    sampleTwoDf: DataFrame,
    comparedColConfig: ColumnConfig,
    evalMethod: String): Double = {
    evalMethod match {
      // [TODO] add other distribution eval approaches here
      case DistributionConstants.KSTEST =>
        KSTest.evaluate(sampleOneDf, sampleTwoDf, comparedColConfig)
    }
  }
}
