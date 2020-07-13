package stats.constants

object DistributionConstants {
  val KSTEST = "kstest"
  val KLDIVERGENCE = "kldivergence"
}

object KSTestConstants {
  val DSHIFT_CUMSUM = "dshift_cumsum"
  val DSHIFT_ECDF = "dshift_ecdf"
  val DSHIFT_ECDF_SAMPLE_ONE = "dshift_ecdf_sample_one"
  val DSHIFT_ECDF_SAMPLE_TWO = "dshift_ecdf_sample_two"
  val DSHIFT_ECDF_DIFFERENCE = "dshift_ecdf_difference"
  val DSHIFT_TOTAL_OBSERVATIONS = "dshift_total_observations"
}

object KLDivergenceConstants {
  val DSHIFT_KLDIV_SAMPLE_FREQUENCY = "dshift_kldiv_sample_frequency"
  val DSHIFT_KLDIV_ORIGIN_PROBA_DISTR = "dshift_kldiv_origin_proba_distr"
  val DSHIFT_KLDIV_CURRENT_PROBA_DISTR = "dshift_kldiv_current_proba_distr"
  val DSHIFT_KLDIV_STATISTIC = "dshift_kldiv_statistic"

  val DSHIFT_KLDIV_UNOBSERVED_SAMPLE_FREQUENCY = 0.0001
}

object KLDivergenceConstants {
  val DSHIFT_KLDIV_SAMPLE_FREQUENCY = "dshift_kldiv_sample_frequency"
  val DSHIFT_KLDIV_ORIGIN_PROBA_DISTR = "dshift_kldiv_origin_proba_distr"
  val DSHIFT_KLDIV_CURRENT_PROBA_DISTR = "dshift_kldiv_current_proba_distr"
  val DSHIFT_KLDIV_STATISTIC = "dshift_kldiv_statistic"

  val DSHIFT_KLDIV_UNOBSERVED_SAMPLE_FREQUENCY = 0.0001
}

object DistributionGeneralConstants {
  val DSHIFT_COMPARED_COL = "dshift_compared_col"

  val DSHIFT_BINNED = "binned"
  val DSHIFT_BINNED_COLUMN = "dshift_binned_column"
}
