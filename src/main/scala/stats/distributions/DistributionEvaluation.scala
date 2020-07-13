package stats.distributions

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.{DataFrame, functions => F}
import stats.configs.{ColumnConfig, OptionsConfig}
import stats.constants.{DistributionConstants, DistributionGeneralConstants}

case class DistributionEvaluationStatus(evalMethod: String, statistic: Double)

object DistributionEvaluation {
  def evaluate(
    originDf: DataFrame,
    currentDf: DataFrame,
    evalMethod: String,
    comparedColConfig: ColumnConfig,
    optionsConfig: OptionsConfig): DistributionEvaluationStatus = {
    val originCol = comparedColConfig.originSampleColumn
    val currentCol = comparedColConfig.currentSampleColumn

    val tmpOriginNonNullsDf = filterOutNulls(originDf, originCol)
    val tmpCurrentNonNullsDf = filterOutNulls(currentDf, currentCol)

    val tmpOriginStandardizedColDf = standardizeColName(tmpOriginNonNullsDf, originCol)
    val tmpCurrentStandardizedColDf = standardizeColName(tmpCurrentNonNullsDf, currentCol)

    val (tmpOriginAndRoundedDf, tmpCurrentAndRoundedDf) = optionsConfig.rounding match {
      case Some(rounding) =>
        (
          roundValues(tmpOriginStandardizedColDf, rounding),
          roundValues(tmpCurrentStandardizedColDf, rounding))
      case None => (tmpOriginStandardizedColDf, tmpCurrentStandardizedColDf)
    }

    val (tmpOriginAndAppliedMethodDf, tmpCurrentAndAppliedMethodDf) = optionsConfig.method match {
      case DistributionGeneralConstants.DSHIFT_BINNED =>
        (
          discretizeDf(
            tmpOriginAndRoundedDf,
            tmpCurrentAndRoundedDf,
            optionsConfig.numOfBin.getOrElse(10)),
          discretizeDf(
            tmpCurrentAndRoundedDf,
            tmpOriginAndRoundedDf,
            optionsConfig.numOfBin.getOrElse(10)))
      case _ => (tmpOriginAndRoundedDf, tmpCurrentAndRoundedDf)
    }

    val statistic = evalMethod match {
      // [TODO] add other distribution comparison methods here
      case DistributionConstants.KSTEST =>
        KSTest.evaluate(tmpOriginAndAppliedMethodDf, tmpCurrentAndAppliedMethodDf, optionsConfig)
      case DistributionConstants.KLDIVERGENCE =>
        KLDivergence.evaluate(
          tmpOriginAndAppliedMethodDf,
          tmpCurrentAndAppliedMethodDf,
          optionsConfig)
    }

    DistributionEvaluationStatus(evalMethod, statistic)
  }

  private def filterOutNulls(df: DataFrame, colName: String): DataFrame =
    df.filter(!F.isnull(F.col(colName)))

  private def standardizeColName(df: DataFrame, colName: String): DataFrame =
    df.select(colName).withColumnRenamed(colName, DistributionGeneralConstants.DSHIFT_COMPARED_COL)

  private def roundValues(df: DataFrame, rounding: Int): DataFrame = {
    df.withColumn(
      DistributionGeneralConstants.DSHIFT_COMPARED_COL,
      F.round(F.col(DistributionGeneralConstants.DSHIFT_COMPARED_COL), rounding))
  }

  private def discretizeDf(
    targetDf: DataFrame,
    complementDf: DataFrame,
    numOfBin: Int): DataFrame = {
    val unionedDf = targetDf.union(complementDf)
    val discretizer = new QuantileDiscretizer()
      .setInputCol(DistributionGeneralConstants.DSHIFT_COMPARED_COL)
      .setOutputCol(DistributionGeneralConstants.DSHIFT_BINNED_COLUMN)
      .setNumBuckets(numOfBin)

    discretizer
      .fit(unionedDf)
      .transform(targetDf)
      .drop(DistributionGeneralConstants.DSHIFT_COMPARED_COL)
      .withColumnRenamed(
        DistributionGeneralConstants.DSHIFT_BINNED_COLUMN,
        DistributionGeneralConstants.DSHIFT_COMPARED_COL)
  }
}
