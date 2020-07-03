package stats

import org.apache.spark.sql.DataFrame
import stats.configs.ColumnConfig

object Util {
  def checkColumnAvailability(
    sample_one_df: DataFrame,
    sample_two_df: DataFrame,
    comparedColConfig: ColumnConfig): Boolean = {}
}
