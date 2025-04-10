package scala.spark.validation.framework.utility

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.spark.validation.framework.common.constant.ApplicationConstant._
import scala.spark.validation.framework.common.logger.Logging

object UtilityFunction extends Logging {
  /**
   * Filter duplicates records from a dataframe based on the specified table name.
   * This function uses window functions to assign row numbers to records and filter out duplicates.
   *
   * @param spark                The spark session to use for processing
   * @param getDistinctDataFrame The dataframe to filter duplicate rows
   * @return
   */
  def filterDuplicateRecords(spark: SparkSession, getDistinctDataFrame: DataFrame): (DataFrame, DataFrame) = {
    logger.info("Started segregating duplicate records")
    val idWindowSpec = Window.partitionBy(Id).orderBy(lit(1))

    val aggregatedDataFrame = getDistinctDataFrame.withColumn(RowNumber, row_number().over(idWindowSpec))

    val duplicateRecords = aggregatedDataFrame.filter(col(RowNumber) > 1).drop(RowNumber)
    val distinctRecords = aggregatedDataFrame.filter(col(RowNumber) === 1).drop(RowNumber)

    val resultDataFrame = if (duplicateRecords.count() > 0) {
      val returnDistinctNonDuplicateDataFrame = distinctRecords.join(duplicateRecords, Seq(Id), "left_anti")
      val returnDuplicateRecord = distinctRecords.join(duplicateRecords, Seq(Id), "left_semi")
      val duplicateDataFrame = duplicateRecords.unionByName(returnDuplicateRecord)

      (returnDistinctNonDuplicateDataFrame, duplicateDataFrame)
    } else {
      (aggregatedDataFrame.drop(RowNumber), spark.emptyDataFrame)
    }
    resultDataFrame
  }

  /**
   * Writes a dataframe to a specified path in parquet format
   *
   * @param writeDataFrame The dataframe to write
   * @param partitionList  The column to partition the data by
   * @param path           The path to write the dataframe to
   */
  def writeDataFrame(writeDataFrame: DataFrame, partitionList: List[String], path: String): Unit = {
    writeDataFrame.write.format("parquet").option("compression", "snappy")
      .mode("overwrite").partitionBy(partitionList: _*).save(path)
  }
}
