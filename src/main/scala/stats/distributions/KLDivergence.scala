package stats.distributions

import org.apache.spark.sql.{DataFrame, functions => F}
import stats.configs.OptionsConfig
import stats.constants.{DistributionGeneralConstants, KLDivergenceConstants}

object KLDivergence extends DistributionComparator {
  override def evaluate(
    originDf: DataFrame,
    currentDf: DataFrame,
    optionsConfig: OptionsConfig): Double = {
    val smoothedOriginSampleDf = smoothSample(originDf, currentDf)
    val smoothedCurrentSampleDf = smoothSample(currentDf, originDf)

    val originSampleProbaDistrDf = computeProbaDistr(
      smoothedOriginSampleDf,
      KLDivergenceConstants.DSHIFT_KLDIV_ORIGIN_PROBA_DISTR)
    val currentSampleProbaDistrDf = computeProbaDistr(
      smoothedCurrentSampleDf,
      KLDivergenceConstants.DSHIFT_KLDIV_CURRENT_PROBA_DISTR)

    computeKLDivStatistic(originSampleProbaDistrDf, currentSampleProbaDistrDf)
  }

  private def smoothSample(targetDf: DataFrame, complementDf: DataFrame): DataFrame = {
    val unObservedTargetSampleDf = complementDf
      .join(targetDf, Seq(DistributionGeneralConstants.DSHIFT_COMPARED_COL), "left_anti")
      .distinct()

    val unObservedTargetSampleCountDf =
      unObservedTargetSampleDf.withColumn(
        KLDivergenceConstants.DSHIFT_KLDIV_SAMPLE_FREQUENCY,
        F.lit(KLDivergenceConstants.DSHIFT_KLDIV_UNOBSERVED_SAMPLE_FREQUENCY))

    val observedTargetSampleCountDf = targetDf
      .groupBy(DistributionGeneralConstants.DSHIFT_COMPARED_COL)
      .count()
      .withColumnRenamed("count", KLDivergenceConstants.DSHIFT_KLDIV_SAMPLE_FREQUENCY)

    val columns = observedTargetSampleCountDf.columns
    unObservedTargetSampleCountDf
      .select(columns.head, columns.tail: _*)
      .union(observedTargetSampleCountDf)
  }

  private def computeProbaDistr(df: DataFrame, probaDistrColName: String): DataFrame = {
    val totalObservations =
      df.agg(F.sum(F.col(KLDivergenceConstants.DSHIFT_KLDIV_SAMPLE_FREQUENCY))).first.get(0)

    df.withColumn(
        probaDistrColName,
        F.col(KLDivergenceConstants.DSHIFT_KLDIV_SAMPLE_FREQUENCY) / F.lit(totalObservations))
      .drop(KLDivergenceConstants.DSHIFT_KLDIV_SAMPLE_FREQUENCY)
  }

  private def computeKLDivStatistic(
    originSampleProbaDistrDf: DataFrame,
    currentSampleProbaDistrDf: DataFrame): Double = {
    val pairOfProbaDistrDf = originSampleProbaDistrDf
      .join(
        currentSampleProbaDistrDf,
        Seq(DistributionGeneralConstants.DSHIFT_COMPARED_COL),
        "inner")

    pairOfProbaDistrDf
      .withColumn(
        KLDivergenceConstants.DSHIFT_KLDIV_STATISTIC,
        F.col(KLDivergenceConstants.DSHIFT_KLDIV_ORIGIN_PROBA_DISTR) * F.log(
          F.col(KLDivergenceConstants.DSHIFT_KLDIV_ORIGIN_PROBA_DISTR) / F.col(
            KLDivergenceConstants.DSHIFT_KLDIV_CURRENT_PROBA_DISTR))
      )
      .drop(
        KLDivergenceConstants.DSHIFT_KLDIV_ORIGIN_PROBA_DISTR,
        KLDivergenceConstants.DSHIFT_KLDIV_CURRENT_PROBA_DISTR)
      .agg(F.sum(F.col(KLDivergenceConstants.DSHIFT_KLDIV_STATISTIC)))
      .first
      .get(0)
      .asInstanceOf[Double]
  }
}
