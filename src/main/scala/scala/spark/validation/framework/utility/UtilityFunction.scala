package scala.spark.validation.framework.utility

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.spark.validation.framework.common.logger.Logging

object UtilityFunction extends Logging {
  def filterDuplicateRecords(spark: SparkSession, getDistinctDataFrame: DataFrame): (DataFrame, DataFrame) = {
    logger.info("Started segregating duplicate records")
    val idWindowSpec = Window.partitionBy("id").orderBy(lit(1))

    val aggregatedDataFrame = getDistinctDataFrame.withColumn("row_number", row_number().over(idWindowSpec))

    val duplicateRecords = aggregatedDataFrame.filter(col("row_number") > 1).drop("row_number")
    val distinctRecords = aggregatedDataFrame.filter(col("row_number") === 1).drop("row_number")

    val resultDataFrame = if (duplicateRecords.count() > 0) {
      val returnDistinctNonDuplicateDataFrame = distinctRecords.join(duplicateRecords, Seq("id"), "left_anti")
      val returnDuplicateRecord = distinctRecords.join(duplicateRecords, Seq("id"), "left_semi")
      val duplicateDataFrame = duplicateRecords.unionByName(returnDuplicateRecord)

      (returnDistinctNonDuplicateDataFrame, duplicateDataFrame)
    } else {
      (aggregatedDataFrame.drop("row_number"), spark.emptyDataFrame)
    }
    resultDataFrame
  }
}
