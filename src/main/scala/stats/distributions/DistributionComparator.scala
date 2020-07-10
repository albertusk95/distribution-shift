package stats.distributions

import org.apache.spark.sql.DataFrame
import stats.configs.ColumnConfig

abstract class DistributionComparator {
  def evaluate(originDf: DataFrame, currentDf: DataFrame, comparedColConfig: ColumnConfig): Double
}
