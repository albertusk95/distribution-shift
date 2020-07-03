package stats.sources

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataReader {
  def readData(): DataFrame
}

class ParquetDataReader(val sourcePath: String) extends DataReader {
  override def readData(): DataFrame = {
    val session = SparkSession.builder.getOrCreate
    session.read
      .parquet(sourcePath)
  }
}

class CsvDataReader(val sourcePath: String) extends DataReader {
  override def readData(): DataFrame = {
    val session = SparkSession.builder().getOrCreate()
    session.read.option("header", "true").option("inferSchema", "true").csv(sourcePath)
  }
}
