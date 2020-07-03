package stats.sources

import stats.constants.SourceConstants

import scala.util.{Failure, Success, Try}

object SourceFactory {
  def of(sourceFormat: String, sourcePath: String): Try[DataReader] = {
    sourceFormat match {
      case SourceConstants.PARQUET =>
        Success(new ParquetDataReader(sourcePath))
      case SourceConstants.CSV =>
        Success(new CsvDataReader(sourcePath))
      case _ => Failure(new ClassNotFoundException(s"DataReader ${sourceFormat} not found"))
    }
  }
}
