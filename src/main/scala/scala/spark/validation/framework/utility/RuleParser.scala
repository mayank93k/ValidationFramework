package scala.spark.validation.framework.utility

import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.spark.validation.framework.common.logger.Logging
import scala.spark.validation.framework.utility.ValidationRuleUtility._

object RuleParser extends Logging {
  def generateValidationExpression(config: Config, section: String): Seq[Column] = {
    logger.info(s"Getting validation expressions for section $section")
    val fields = config.getConfigList(section).asScala
    fields.flatMap { field =>
      val fieldName = field.getString("name")
      val validations = field.getStringList("validationRule").asScala
      val flag = field.getBoolean("flag")
      if (flag) {
        val validationExpression = validations.map {
          case "isNotNull" => (isNotNull(fieldName), "isNotNull")
          case "isNotBlank" => (isNotBlank(fieldName), "isNotBlank")
          case "notContainsInvalidValue" => (notContainsInvalidValue(fieldName), "notContainsInvalidValue")
          case "notContainsBlankString" => (notContainsBlankString(fieldName), "notContainsBlankString")
        }
        val validationExpr = validationExpression.map(_._1).reduce(_ && _)
        val isValidColumn = when(validationExpr, lit("Yes")).otherwise(lit("No")).alias(s"${fieldName}_isValid")

        val failedValidations = validationExpression.collect {
          case (expr, rule) if rule != null => when(!expr, rule)
        }
        val removeNullsUDF = udf((array: Seq[String]) => array.filter(_ != null))
        val invalidStatusColumn = when(!validationExpr, removeNullsUDF(array(failedValidations: _*)).cast("array<string>"))
          .alias(s"${fieldName}_invalidStatus")

        Seq(isValidColumn, invalidStatusColumn)
      } else {
        Seq.empty
      }

    }
  }

}
