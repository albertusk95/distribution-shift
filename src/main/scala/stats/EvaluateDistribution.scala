package stats

import stats.configs.{ConfigUtils, DistributionEvalConfig}
import stats.distributions.DistributionEvaluation
import stats.sources.SourceFactory

import scala.util.{Failure, Try}

object EvaluateDistribution {
  def main(args: Array[String]): Unit =
    RunWithSpark.run(() => process(args))

  def process(args: Array[String]): Unit = {
    val configs = args
      .map(arg => readConfig(arg).get)

    args
      .zip(configs)
      .toStream
      .map {
        case (config_path: String, config: DistributionEvalConfig) =>
          processOne(config_path, config)
      }
      .toList
  }

  private def processOne(config_path: String, config: DistributionEvalConfig): Unit = {
    val sample_one_df =
      SourceFactory.of(config.source.format, config.source.path_to_first_sample).get.readData()
    val sample_two_df =
      SourceFactory.of(config.source.format, config.source.path_to_second_sample).get.readData()

    if (Util.checkColumnAvailability(sample_one_df, sample_two_df, config.compared_col)) {
      DistributionEvaluation.evaluate(
        sample_one_df,
        sample_two_df,
        config.compared_col,
        config.eval_method)
    }
    else {
      throw new Exception("One or more columns to compare doesn't exist in the data")
    }
  }

  private def readConfig(file: String): Try[DistributionEvalConfig] =
    Try(ConfigUtils.loadConfig(file)).recover {
      case e => throw new Error(s"Error parsing file: $file", e)
    }
}
