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

    var tmpOriginDf = originDf
      .filter(!F.isnull(F.col(originCol)))
      .withColumnRenamed(originCol, DistributionGeneralConstants.DSHIFT_COMPARED_COL)
      .select(DistributionGeneralConstants.DSHIFT_COMPARED_COL)
    var tmpCurrentDf = currentDf
      .filter(!F.isnull(F.col(currentCol)))
      .withColumnRenamed(currentCol, DistributionGeneralConstants.DSHIFT_COMPARED_COL)
      .select(DistributionGeneralConstants.DSHIFT_COMPARED_COL)

    optionsConfig.rounding match {
      case Some(rounding) =>
        tmpOriginDf = tmpOriginDf.withColumn(
          DistributionGeneralConstants.DSHIFT_COMPARED_COL,
          F.round(F.col(DistributionGeneralConstants.DSHIFT_COMPARED_COL), rounding))
        tmpCurrentDf = tmpCurrentDf.withColumn(
          DistributionGeneralConstants.DSHIFT_COMPARED_COL,
          F.round(F.col(DistributionGeneralConstants.DSHIFT_COMPARED_COL), rounding))
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
}
