package stats.distributions

import org.apache.spark.sql.DataFrame
import stats.configs.{ColumnConfig, OptionsConfig}

abstract class DistributionComparator {
  def evaluate(originDf: DataFrame, currentDf: DataFrame, optionsConfig: OptionsConfig): Double
}
