package stats.configs

case class DistributionEvalConfig(
  eval_method: String,
  compared_col: ColumnConfig,
  source: SourceConfig)
