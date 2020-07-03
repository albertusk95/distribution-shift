package stats.distributions

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.expressions.Window
import stats.configs.ColumnConfig
import stats.constants.KSTestConstants

object KSTest {
  def evaluate(
    sampleOneDf: DataFrame,
    sampleTwoDf: DataFrame,
    comparedColConfig: ColumnConfig): Double = {
    val sampleOneColumn = comparedColConfig.sampleOneColumn
    val sampleTwoColumn = comparedColConfig.sampleTwoColumn

    val cumSumSampleOneDf = computeCumulativeSum(sampleOneDf, sampleOneColumn)
    val cumSumSampleTwoDf = computeCumulativeSum(sampleTwoDf, sampleTwoColumn)

    val empiricalCumDistFuncSampleOneDf = computeEmpiricalCDF(cumSumSampleOneDf)
    val empiricalCumDistFuncSampleTwoDf = computeEmpiricalCDF(cumSumSampleTwoDf)

    val distanceEmpiricalCumDistFuncSampleOneDf =
      computeEmpiricalCDFDistance(empiricalCumDistFuncSampleOneDf, empiricalCumDistFuncSampleTwoDf)
  }

  private def computeCumulativeSum(df: DataFrame, column: String): DataFrame = {
    val window = Window.orderBy(column)
    df.withColumn(KSTestConstants.CUMSUM, F.count(column).over(window))
  }

  private def computeEmpiricalCDF(df: DataFrame): DataFrame = {
    val totalObservations = df.agg(F.max(KSTestConstants.CUMSUM)).head.get(0)
    df.withColumn(KSTestConstants.ECDF, F.col(KSTestConstants.CUMSUM) / F.lit(totalObservations))
  }

  private def computeEmpiricalCDFDistance(
    ecdfSampleOne: DataFrame,
    ecdfSampleTwo: DataFrame): DataFrame = {}
}
