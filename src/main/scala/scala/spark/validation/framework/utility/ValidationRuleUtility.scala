package scala.spark.validation.framework.utility

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, trim}

object ValidationRuleUtility {

  def isNotNull(colName: String): Column = col(colName).isNotNull

  def isNotBlank(colName: String): Column = trim(col(colName)) =!= ""

  def notContainsInvalidValue(colName: String): Column = trim(col(colName)) =!= "NO_VALUE"

  def notContainsBlankString(colName: String): Column = trim(col(colName)) =!= "BLANK"
}
