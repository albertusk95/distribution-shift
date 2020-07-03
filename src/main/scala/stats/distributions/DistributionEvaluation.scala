package stats.distributions

import org.apache.spark.sql.DataFrame
import stats.configs.ColumnConfig
import stats.constants.DistributionConstants

object DistributionEvaluation {
  def evaluate(
    sample_one_df: DataFrame,
    sample_two_df: DataFrame,
    comparedColConfig: ColumnConfig,
    evalMethod: String): Double = {
    evalMethod match {
      // [TODO] add other distribution eval approaches here
      case DistributionConstants.KSTEST => evaluateKSTest(sample_one_df, sample_two_df)
    }
  }

  private def evaluateKSTest(sample_one_df: DataFrame, sample_two_df: DataFrame): Double =
    0.5
}
