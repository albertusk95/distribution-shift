package stats

import org.apache.spark.sql.SparkSession

object RunWithSpark {
  private def initializeSpark(): SparkSession = {
    SparkSession.builder
      .master("local[*]")
      .appName("DISTRIBUTION_EVALUATION")
      .getOrCreate
  }

  private def stopSpark(): Unit = SparkSession.builder.getOrCreate.stop

  def run(code: () => Unit): Unit = {
    initializeSpark()
    try {
      code()
    }
    finally {
      stopSpark()
    }
  }
}
