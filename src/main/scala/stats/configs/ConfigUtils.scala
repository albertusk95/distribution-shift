package stats.configs

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.parser.decode

import scala.io.Source.fromFile

object ConfigUtils {
  implicit private val customConfig: Configuration =
    Configuration.default.withSnakeCaseMemberNames

  private def readFromFile(filePath: String): String = {
    val source = fromFile(filePath)
    val content = source.mkString
    source.close()
    content
  }

  def loadConfig(configPath: String): DistributionEvalConfig = {
    val distributionEvalConfig = readFromFile(configPath)
    val decodingResult = decode[DistributionEvalConfig](distributionEvalConfig)

    decodingResult match {
      case Right(config) => config
      case Left(e)       => throw e
    }
  }
}
