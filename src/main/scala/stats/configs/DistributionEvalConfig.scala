package stats.configs

case class DistributionEvalConfig(
  evalMethod: String,
  comparedCol: ColumnConfig,
  source: SourceConfig,
  options: OptionsConfig)
