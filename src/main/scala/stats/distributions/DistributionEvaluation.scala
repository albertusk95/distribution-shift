package stats.distributions

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

    var tmpOriginDf = filterOutNulls(originDf, originCol)
    var tmpCurrentDf = filterOutNulls(currentDf, currentCol)

    tmpOriginDf = standardizeColName(tmpOriginDf, originCol)
    tmpCurrentDf = standardizeColName(tmpCurrentDf, currentCol)

    optionsConfig.rounding match {
      case Some(rounding) =>
        tmpOriginDf = roundValues(tmpOriginDf, rounding)
        tmpCurrentDf = roundValues(tmpCurrentDf, rounding)
      case None =>
    }

    val statistic = evalMethod match {
      // [TODO] add other distribution eval approaches here
      case DistributionConstants.KSTEST =>
        KSTest.evaluate(tmpOriginDf, tmpCurrentDf, optionsConfig)
      case DistributionConstants.KLDIVERGENCE =>
        KLDivergence.evaluate(tmpOriginDf, tmpCurrentDf, optionsConfig)
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
}
