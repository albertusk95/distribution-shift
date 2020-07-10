package stats.distributions

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import stats.configs.ColumnConfig
import stats.constants.{DistributionGeneralConstants, KSTestConstants}

object KSTest extends DistributionComparator {
  def evaluate(
    originDf: DataFrame,
    currentDf: DataFrame,
    comparedColConfig: ColumnConfig): Double = {
    val cumSumSampleOneDf = computeCumulativeSum(originDf)
    val cumSumSampleTwoDf = computeCumulativeSum(currentDf)

    val empiricalCumDistFuncSampleOneDf =
      computeEmpiricalCDF(cumSumSampleOneDf, KSTestConstants.ECDF_SAMPLE_ONE)
    val empiricalCumDistFuncSampleTwoDf =
      computeEmpiricalCDF(cumSumSampleTwoDf, KSTestConstants.ECDF_SAMPLE_TWO)

    val diffEmpiricalCumDistFuncDf =
      computeEmpiricalCDFDifference(
        empiricalCumDistFuncSampleOneDf,
        empiricalCumDistFuncSampleTwoDf)

    getMaxECDFDifference(diffEmpiricalCumDistFuncDf)
  }

  private def computeCumulativeSum(df: DataFrame): DataFrame = {
    val window = Window.orderBy(DistributionGeneralConstants.DSHIFT_COMPARED_COL)
    df.withColumn(
      KSTestConstants.CUMSUM,
      F.count(DistributionGeneralConstants.DSHIFT_COMPARED_COL).over(window))
  }

  private def computeEmpiricalCDF(df: DataFrame, renamedECDF: String): DataFrame = {
    val totalObservations = df.agg(F.max(KSTestConstants.CUMSUM)).head.get(0)
    df.withColumn(renamedECDF, F.col(KSTestConstants.CUMSUM) / F.lit(totalObservations))
      .select(DistributionGeneralConstants.DSHIFT_COMPARED_COL, renamedECDF)
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
