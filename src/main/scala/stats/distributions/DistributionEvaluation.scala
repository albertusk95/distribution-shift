package stats.distributions

import org.apache.spark.sql.DataFrame
import stats.configs.ColumnConfig
import stats.constants.DistributionConstants

case class DistributionEvaluationStatus(evalMethod: String, statistic: Double)

object DistributionEvaluation {
  def evaluate(
    sampleOneDf: DataFrame,
    sampleTwoDf: DataFrame,
    comparedColConfig: ColumnConfig,
    evalMethod: String): DistributionEvaluationStatus = {
    val statistic = evalMethod match {
      // [TODO] add other distribution eval approaches here
      case DistributionConstants.KSTEST =>
        KSTest.evaluate(sampleOneDf, sampleTwoDf, comparedColConfig)
    }

    DistributionEvaluationStatus(evalMethod, statistic)
  }
}
