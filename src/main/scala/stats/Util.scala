package stats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import stats.configs.ColumnConfig

object Util {
  def areColumnsAvailable(
    sampleOneDf: DataFrame,
    sampleTwoDf: DataFrame,
    comparedColConfig: ColumnConfig): Boolean = {
    val sampleOneCol = comparedColConfig.sampleOneColumn
    val sampleTwoCol = comparedColConfig.sampleTwoColumn

    sampleOneDf.columns.contains(sampleOneCol) && sampleTwoDf.columns.contains(sampleTwoCol)
  }

  def areNumericTypeColumns(
    sampleOneDf: DataFrame,
    sampleTwoDf: DataFrame,
    comparedColConfig: ColumnConfig): Boolean = {
    val sampleOneCol = comparedColConfig.sampleOneColumn
    val sampleTwoCol = comparedColConfig.sampleTwoColumn

    sampleOneDf.schema(sampleOneCol).dataType != StringType && sampleTwoDf
      .schema(sampleTwoCol)
      .dataType != StringType
  }
}
