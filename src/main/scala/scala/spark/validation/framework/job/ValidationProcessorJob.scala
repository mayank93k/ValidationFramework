package scala.spark.validation.framework.job

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.spark.validation.framework.alert.UtilityFunction.addBeautifiedHeaderMessage
import scala.spark.validation.framework.alert.{AlertMessage, UtilityFunction}
import scala.spark.validation.framework.common.logger.Logging
import scala.spark.validation.framework.validation.ProcessRuleValidator

/**
 * The ValidationProcessorJob object is responsible for initializing a spark session and running validation process for sources.
 * It reads configuration files, executes the validation logic and handles any exceptions that occur during the process.
 * Finally, it sends email alerts and stops the spark session.
 */
object ValidationProcessorJob extends Logging {
  def main(args: Array[String]): Unit = {
    logger.info("Spark Session Initialization")
    val spark = SparkSession.builder().master("local[*]").appName("ValidationProcessor").getOrCreate()
    val readData = spark.read.option("header", "true").csv("src/main/resources/input/employee_data.csv")

    val config = ConfigFactory.load("validationParserConfig.json")
      .withFallback(ConfigFactory.load("jobConfig.conf"))
    try {
      ProcessRuleValidator(config, spark, readData)
      logger.info("Validation Processor Executed Successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Error during validation for source", e)
        AlertMessage.clearMessage()
        addBeautifiedHeaderMessage("Validation Failed")
        AlertMessage.addMessage(s"Error during processing validation: ${e.getMessage}")
        UtilityFunction.AlertProcessor()
    } finally {
      AlertMessage.addMessage("Validation Processing completed")
      UtilityFunction.AlertProcessor()
      spark.catalog.clearCache()
      spark.stop()
    }
  }
}
