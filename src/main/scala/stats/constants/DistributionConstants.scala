package stats.constants

object DistributionConstants {
  val KSTEST = "kstest"
  val KLDIVERGENCE = "kldivergence"
}

object KSTestConstants {
  val CUMSUM = "cumsum"
  val ECDF = "ecdf"
  val ECDF_SAMPLE_ONE = "ecdf_sample_one"
  val ECDF_SAMPLE_TWO = "ecdf_sample_two"
  val ECDF_DIFFERENCE = "ecdf_difference"
  val KSTEST_COMPARED_COLUMN = "compared_column"
  val TOTAL_OBSERVATIONS = "total_observations"
}

object KLDivergenceConstants {
  val DSHIFT_KLDIV_SAMPLE_FREQUENCY = "dshift_kldiv_sample_frequency"
  val DSHIFT_KLDIV_PROBA_DISTR = "dshift_kldiv_proba_distr"
  val DSHIFT_KLDIV_UNOBSERVED_SAMPLE_FREQUENCY = 0.0001
}

object DistributionGeneralConstants {
  val DSHIFT_COMPARED_COL = "dshift_compared_col"
}
