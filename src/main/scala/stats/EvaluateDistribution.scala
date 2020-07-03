package stats

import stats.configs.{ConfigUtils, DistributionEvalConfig}
import stats.distributions.DistributionEvaluation
import stats.sources.SourceFactory

import scala.util.Try

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

    val statistics =
      DistributionEvaluation.evaluate(sample_one_df, sample_two_df, config.eval_method)
  }

  private def readConfig(file: String): Try[DistributionEvalConfig] =
    Try(ConfigUtils.loadConfig(file)).recover {
      case e => throw new Error(s"Error parsing file: $file", e)
    }
}
