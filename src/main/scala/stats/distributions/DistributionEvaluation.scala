package stats.distributions

import org.apache.spark.sql.{DataFrame, functions => F}
import stats.configs.ColumnConfig
import stats.constants.{DistributionConstants, DistributionGeneralConstants}

case class DistributionEvaluationStatus(evalMethod: String, statistic: Double)

object DistributionEvaluation {
  def evaluate(
    originDf: DataFrame,
    currentDf: DataFrame,
    comparedColConfig: ColumnConfig,
    evalMethod: String): DistributionEvaluationStatus = {

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

    val statistic = evalMethod match {
      // [TODO] add other distribution eval approaches here
      case DistributionConstants.KSTEST =>
        KSTest.evaluate(tmpOriginDf, tmpCurrentDf, comparedColConfig)
    }

    DistributionEvaluationStatus(evalMethod, statistic)
  }
}
