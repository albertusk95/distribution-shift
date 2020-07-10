package stats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import stats.configs.ColumnConfig

object Util {
  def areColumnsAvailable(
    originDf: DataFrame,
    currentDf: DataFrame,
    comparedColConfig: ColumnConfig): Boolean = {
    val originSampleCol = comparedColConfig.originSampleColumn
    val currentSampleCol = comparedColConfig.currentSampleColumn

    originDf.columns.contains(originSampleCol) && currentDf.columns.contains(currentSampleCol)
  }

  def areNumericTypeColumns(
    originDf: DataFrame,
    currentDf: DataFrame,
    comparedColConfig: ColumnConfig): Boolean = {
    val originSampleCol = comparedColConfig.originSampleColumn
    val currentSampleCol = comparedColConfig.currentSampleColumn

    originDf.schema(originSampleCol).dataType != StringType && currentDf
      .schema(currentSampleCol)
      .dataType != StringType
  }
}
