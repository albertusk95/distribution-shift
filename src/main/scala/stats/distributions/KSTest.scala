package stats.distributions

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import stats.configs.ColumnConfig
import stats.constants.KSTestConstants

object KSTest {
  def evaluate(
    sampleOneDf: DataFrame,
    sampleTwoDf: DataFrame,
    comparedColConfig: ColumnConfig): Double = {
    val sampleOneColumn = comparedColConfig.sampleOneColumn
    val sampleTwoColumn = comparedColConfig.sampleTwoColumn

    val sampleOneWithEqualizedComparedColDf =
      equalizeComparedColumnName(sampleOneDf, sampleOneColumn)
    val sampleTwoWithEqualizedComparedColDf =
      equalizeComparedColumnName(sampleTwoDf, sampleTwoColumn)

    val cumSumSampleOneDf = computeCumulativeSum(
      sampleOneWithEqualizedComparedColDf.select(KSTestConstants.KSTEST_COMPARED_COLUMN))
    val cumSumSampleTwoDf = computeCumulativeSum(
      sampleTwoWithEqualizedComparedColDf.select(KSTestConstants.KSTEST_COMPARED_COLUMN))

    val empiricalCumDistFuncSampleOneDf = computeEmpiricalCDF(cumSumSampleOneDf)
      .select(KSTestConstants.KSTEST_COMPARED_COLUMN, KSTestConstants.ECDF)
      .withColumnRenamed(KSTestConstants.ECDF, KSTestConstants.ECDF_SAMPLE_ONE)
    val empiricalCumDistFuncSampleTwoDf = computeEmpiricalCDF(cumSumSampleTwoDf)
      .select(KSTestConstants.KSTEST_COMPARED_COLUMN, KSTestConstants.ECDF)
      .withColumnRenamed(KSTestConstants.ECDF, KSTestConstants.ECDF_SAMPLE_TWO)

    val diffEmpiricalCumDistFuncDf =
      computeEmpiricalCDFDifference(
        empiricalCumDistFuncSampleOneDf,
        empiricalCumDistFuncSampleTwoDf)

    getMaxECDFDifference(diffEmpiricalCumDistFuncDf)
  }

  private def equalizeComparedColumnName(df: DataFrame, column: String): DataFrame =
    df.withColumn(KSTestConstants.KSTEST_COMPARED_COLUMN, F.col(column))

  private def computeCumulativeSum(df: DataFrame): DataFrame = {
    val window = Window.orderBy(KSTestConstants.KSTEST_COMPARED_COLUMN)
    df.withColumn(
      KSTestConstants.CUMSUM,
      F.count(KSTestConstants.KSTEST_COMPARED_COLUMN).over(window))
  }

  private def computeEmpiricalCDF(df: DataFrame): DataFrame = {
    val totalObservations = df.agg(F.max(KSTestConstants.CUMSUM)).head.get(0)
    df.withColumn(KSTestConstants.ECDF, F.col(KSTestConstants.CUMSUM) / F.lit(totalObservations))
  }

  private def computeEmpiricalCDFDifference(
    ecdfSampleOne: DataFrame,
    ecdfSampleTwo: DataFrame): DataFrame = {
    val sampleOneWithECDFSampleTwo =
      ecdfSampleOne.withColumn(KSTestConstants.ECDF_SAMPLE_TWO, F.lit(null))
    val sampleTwoWithECDFSampleOne =
      ecdfSampleTwo.withColumn(KSTestConstants.ECDF_SAMPLE_ONE, F.lit(null))
    val unionedSamples = sampleOneWithECDFSampleTwo.unionByName(sampleTwoWithECDFSampleOne)

    val windowFillers: Seq[WindowSpec] = getWindowFillers

    val filledUnionedSamples =
      fillNullInUnionedSamples(unionedSamples, windowFillers)
    filledUnionedSamples
      .withColumn(
        KSTestConstants.ECDF_DIFFERENCE,
        F.abs(F.col(KSTestConstants.ECDF_SAMPLE_ONE) - F.col(KSTestConstants.ECDF_SAMPLE_TWO)))
  }

  private def getWindowFillers: Seq[WindowSpec] = {
    val windowFillerSampleOne = Window
      .orderBy(
        Seq(
          F.col(KSTestConstants.KSTEST_COMPARED_COLUMN),
          F.col(KSTestConstants.ECDF_SAMPLE_ONE).asc_nulls_last): _*)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val windowFillerSampleTwo = Window
      .orderBy(
        Seq(
          F.col(KSTestConstants.KSTEST_COMPARED_COLUMN),
          F.col(KSTestConstants.ECDF_SAMPLE_TWO).asc_nulls_last): _*)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    Seq(windowFillerSampleOne, windowFillerSampleTwo)
  }

  private def fillNullInUnionedSamples(df: DataFrame, windowFillers: Seq[WindowSpec]): DataFrame = {
    val windowFillerSampleOne = windowFillers.head
    val windowFillerSampleTwo = windowFillers.tail.head

    df.withColumn(
        KSTestConstants.ECDF_SAMPLE_ONE,
        F.last(KSTestConstants.ECDF_SAMPLE_ONE, ignoreNulls = true).over(windowFillerSampleOne)
      )
      .withColumn(
        KSTestConstants.ECDF_SAMPLE_TWO,
        F.last(KSTestConstants.ECDF_SAMPLE_TWO, ignoreNulls = true).over(windowFillerSampleTwo)
      )
      .na
      .fill(0.0)
  }

  private def getMaxECDFDifference(df: DataFrame): Double =
    df.agg(F.max(KSTestConstants.ECDF_DIFFERENCE)).head.get(0).asInstanceOf[Double]
}
