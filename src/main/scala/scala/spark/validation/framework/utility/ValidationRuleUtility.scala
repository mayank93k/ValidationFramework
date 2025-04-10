package scala.spark.validation.framework.utility

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, trim}

object ValidationRuleUtility {
  /**
   * Returns a column representing a validation rule that checks if a column is not null.
   *
   * @param colName The name of the column to validate.
   * @return A column representing the validation rule.
   */
  def isNotNull(colName: String): Column = col(colName).isNotNull

  /**
   * Returns a column representing a validation rule that checks if a column is not blank.
   *
   * @param colName The name of the column to validate.
   * @return A column representing the validation rule.
   */
  def isNotBlank(colName: String): Column = trim(col(colName)) =!= ""

  /**
   * Returns a column representing a validation rule that checks if a column does not contains an invalid value.
   *
   * @param colName The name of the column to validate.
   * @return A column representing the validation rule.
   */
  def notContainsInvalidValue(colName: String): Column = trim(col(colName)) =!= "NO_VALUE"

  /**
   * Returns a column representing a validation rule that checks if a column does not contains a blank string keyword.
   *
   * @param colName The name of the column to validate
   * @return A column representing the validation rule.
   */
  def notContainsBlankString(colName: String): Column = trim(col(colName)) =!= "BLANK"
}
