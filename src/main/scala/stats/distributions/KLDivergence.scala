package stats.distributions

import org.apache.spark.sql.DataFrame
import stats.configs.OptionsConfig

object KLDivergence extends DistributionComparator {
  def evaluate(originDf: DataFrame, currentDf: DataFrame, optionsConfig: OptionsConfig): Double = {}
}
