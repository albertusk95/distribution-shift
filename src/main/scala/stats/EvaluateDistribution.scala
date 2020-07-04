package stats

import org.apache.spark.sql.SparkSession
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
    val sampleOneDf =
      SourceFactory.of(config.source.format, config.source.pathToFirstSample).get.readData()
    val sampleTwoDf =
      SourceFactory.of(config.source.format, config.source.pathToSecondSample).get.readData()

    if (
      Util.areColumnsAvailable(sampleOneDf, sampleTwoDf, config.comparedCol)
      && Util.areNumericTypeColumns(sampleOneDf, sampleTwoDf, config.comparedCol)
    ) {
      val evalStatus = DistributionEvaluation.evaluate(
        sampleOneDf,
        sampleTwoDf,
        config.comparedCol,
        config.evalMethod)

      SparkSession.builder.getOrCreate.createDataFrame(Seq(evalStatus)).show()
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
