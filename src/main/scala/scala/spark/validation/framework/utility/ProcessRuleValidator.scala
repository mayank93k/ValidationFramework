package scala.spark.validation.framework.utility

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, current_date, date_format, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.spark.validation.framework.common.logger.Logging
import scala.spark.validation.framework.utility.RuleParser.generateValidationExpression
import scala.spark.validation.framework.utility.UtilityFunction.filterDuplicateRecords

object ProcessRuleValidator extends Logging {
  def apply(config: Config, spark: SparkSession, dataFrame: DataFrame): Unit = {
    logger.info("Validation Results for Tables")

    val tableDetails = config.getStringList("targetTable").asScala
    tableDetails.foreach { table =>
      val tableName = table
      logger.info(s"$tableName is getting processed")
      val addPartitionDateColumn = dataFrame.withColumn("currentDate", current_date())
      val convertedDateDataFrame = addPartitionDateColumn.withColumn("currentDate",
        date_format(to_date(col("currentDate"), "yyyy-MM-dd"), "yyyyMMdd"))

      val getDistinctDataFrame = convertedDateDataFrame.distinct()
      val filterDuplicateData = filterDuplicateRecords(spark, getDistinctDataFrame)
      val validationExpression = generateValidationExpression(config, s"validationFields.$tableName")
      val processRuleDataFrame = filterDuplicateData._1.select(validationExpression :+ col("*"): _*)

      processRuleDataFrame.write.format("parquet").option("compression", "snappy")
        .mode("overwrite").partitionBy("currentDate", "department").save("src/main/resources/output/status/")

      if (!filterDuplicateData._2.isEmpty) {
        filterDuplicateData._2.write.format("parquet").option("compression", "snappy")
          .mode("overwrite").partitionBy("currentDate", "department").save("src/main/resources/output/duplicate/")
      }

      filterDuplicateData._1.write.format("parquet").option("compression", "snappy")
        .mode("overwrite").partitionBy("currentDate", "department").save("src/main/resources/output/non_duplicate/")
    }
  }
}
