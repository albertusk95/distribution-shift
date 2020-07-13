package stats.distributions

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import stats.configs.OptionsConfig
import stats.constants.{DistributionGeneralConstants, KSTestConstants}

object KSTest extends DistributionComparator {
  def evaluate(originDf: DataFrame, currentDf: DataFrame, optionsConfig: OptionsConfig): Double = {
    val cumSumSampleOneDf = computeCumulativeSum(originDf)
    val cumSumSampleTwoDf = computeCumulativeSum(currentDf)

    val empiricalCumDistFuncSampleOneDf =
      computeEmpiricalCDF(cumSumSampleOneDf, KSTestConstants.DSHIFT_ECDF_SAMPLE_ONE)
    val empiricalCumDistFuncSampleTwoDf =
      computeEmpiricalCDF(cumSumSampleTwoDf, KSTestConstants.DSHIFT_ECDF_SAMPLE_TWO)

    val diffEmpiricalCumDistFuncDf =
      computeEmpiricalCDFDifference(
        empiricalCumDistFuncSampleOneDf,
        empiricalCumDistFuncSampleTwoDf)

    getMaxECDFDifference(diffEmpiricalCumDistFuncDf)
  }

  private def computeCumulativeSum(df: DataFrame): DataFrame = {
    val window = Window.orderBy(DistributionGeneralConstants.DSHIFT_COMPARED_COL)
    df.withColumn(
      KSTestConstants.DSHIFT_CUMSUM,
      F.count(DistributionGeneralConstants.DSHIFT_COMPARED_COL).over(window))
  }

  private def computeEmpiricalCDF(df: DataFrame, renamedECDF: String): DataFrame = {
    val totalObservations = df.agg(F.max(KSTestConstants.DSHIFT_CUMSUM)).head.get(0)
    df.withColumn(renamedECDF, F.col(KSTestConstants.DSHIFT_CUMSUM) / F.lit(totalObservations))
      .select(DistributionGeneralConstants.DSHIFT_COMPARED_COL, renamedECDF)
  }

  private def computeEmpiricalCDFDifference(
    ecdfSampleOne: DataFrame,
    ecdfSampleTwo: DataFrame): DataFrame = {
    val sampleOneWithECDFSampleTwo =
      ecdfSampleOne.withColumn(KSTestConstants.DSHIFT_ECDF_SAMPLE_TWO, F.lit(null))
    val sampleTwoWithECDFSampleOne =
      ecdfSampleTwo.withColumn(KSTestConstants.DSHIFT_ECDF_SAMPLE_ONE, F.lit(null))
    val unionedSamples = sampleOneWithECDFSampleTwo.unionByName(sampleTwoWithECDFSampleOne)

    val windowFillers: Seq[WindowSpec] = getWindowFillers

    val filledUnionedSamples =
      fillNullInUnionedSamples(unionedSamples, windowFillers)

    filledUnionedSamples
      .withColumn(
        KSTestConstants.DSHIFT_ECDF_DIFFERENCE,
        F.abs(
          F.col(KSTestConstants.DSHIFT_ECDF_SAMPLE_ONE) - F.col(
            KSTestConstants.DSHIFT_ECDF_SAMPLE_TWO)))
  }

  private def getWindowFillers: Seq[WindowSpec] = {
    val windowFillerSampleOne = Window
      .orderBy(
        Seq(
          F.col(DistributionGeneralConstants.DSHIFT_COMPARED_COL),
          F.col(KSTestConstants.DSHIFT_ECDF_SAMPLE_ONE).asc_nulls_last): _*)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val windowFillerSampleTwo = Window
      .orderBy(
        Seq(
          F.col(DistributionGeneralConstants.DSHIFT_COMPARED_COL),
          F.col(KSTestConstants.DSHIFT_ECDF_SAMPLE_TWO).asc_nulls_last): _*)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    Seq(windowFillerSampleOne, windowFillerSampleTwo)
  }

  private def fillNullInUnionedSamples(df: DataFrame, windowFillers: Seq[WindowSpec]): DataFrame = {
    val windowFillerSampleOne = windowFillers.head
    val windowFillerSampleTwo = windowFillers.tail.head

    df.withColumn(
        KSTestConstants.DSHIFT_ECDF_SAMPLE_ONE,
        F.last(KSTestConstants.DSHIFT_ECDF_SAMPLE_ONE, ignoreNulls = true)
          .over(windowFillerSampleOne)
      )
      .withColumn(
        KSTestConstants.DSHIFT_ECDF_SAMPLE_TWO,
        F.last(KSTestConstants.DSHIFT_ECDF_SAMPLE_TWO, ignoreNulls = true)
          .over(windowFillerSampleTwo)
      )
      .na
      .fill(0.0)
  }

  private def getMaxECDFDifference(df: DataFrame): Double =
    df.agg(F.max(KSTestConstants.DSHIFT_ECDF_DIFFERENCE)).head.get(0).asInstanceOf[Double]
}
