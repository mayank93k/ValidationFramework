package scala.spark.validation.framework.validation

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, current_date, date_format, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.spark.validation.framework.alert.AlertMessage
import scala.spark.validation.framework.alert.UtilityFunction.addBeautifiedHeaderMessage
import scala.spark.validation.framework.common.logger.Logging
import scala.spark.validation.framework.utility.RuleParser.generateValidationExpression
import scala.spark.validation.framework.utility.UtilityFunction.{filterDuplicateRecords, writeDataFrame}

object ProcessRuleValidator extends Logging {
  /**
   * This method is used to validate the process rule for all sources
   *
   * @param config    The configuration object containing validation rules.
   * @param spark     The spark session
   * @param dataFrame Input data frame
   */
  def apply(config: Config, spark: SparkSession, dataFrame: DataFrame): Unit = {
    logger.info("Validation Results for Tables")
    addBeautifiedHeaderMessage("Validation Result and Duplicate Check")

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
      val partitionColumns = config.getStringList("partitionColumn").asScala.toList
      val statusPath = config.getString("statusPath")
      val duplicatePath = config.getString("duplicatePath")
      val nonDuplicatePath = config.getString("nonDuplicatePath")
      AlertMessage.addMessage(s"List of source which is getting processed is: $tableName \n")

      writeDataFrame(processRuleDataFrame, partitionColumns, statusPath)

      if (!filterDuplicateData._2.isEmpty) {
        writeDataFrame(filterDuplicateData._2, partitionColumns, duplicatePath)
      }

      writeDataFrame(filterDuplicateData._1, partitionColumns, nonDuplicatePath)
    }
  }
}
