package scala.spark.validation.framework.alert

import scala.spark.validation.framework.common.logger.Logging

object UtilityFunction extends Logging {
  /**
   * Processes alert messages and sends them for parsing.
   * This function retrieves the combined alert message from `AlertMessage`,
   * and passes it to `EmailAlertParser`. If no messages are present,
   * it logs a message and exits. It also includes basic error handling
   * to prevent failures if `EmailAlertParser` throws an exception.
   */
  def AlertProcessor(): Unit = {
    val combinedMessage = AlertMessage.getCombinedMessage

    if (combinedMessage.nonEmpty) {
      try {
        EmailAlert(combinedMessage)
      } catch {
        case e: Exception =>
          logger.error(s"Error processing alert: ${e.getMessage}")
      }
    } else {
      logger.warn("No alerts to process.")
    }
  }

  /**
   * Adds a beautified header message with proper HTML formatting for email compatibility.
   *
   * @param message The message to be formatted and added.
   *
   *                This method:
   *                - Centers the message within a fixed width of 125 characters.
   *                - Wraps it with a border-like design for visibility.
   *                - Uses an HTML `<div>` instead of `<pre>` for better Gmail rendering.
   */
  def addBeautifiedHeaderMessage(message: String): Unit = {
    val totalWidth = 50 // Total width including borders
    val borderChar = "_" // Character for the top and bottom border

    // Calculate padding to center the message correctly
    val totalPadding = totalWidth - message.length - 2
    val leftPaddingSize = totalPadding / 2
    val rightPaddingSize = totalPadding - leftPaddingSize // Ensure both sides sum correctly

    val leftPadding = " " * leftPaddingSize
    val rightPadding = " " * rightPaddingSize

    // Construct the beautified message ensuring both borders are included
    val beautifiedMessage = s"#$leftPadding$message$rightPadding#"
    val pattern = borderChar * totalWidth // Top and bottom border

    // Construct the plain text version
    val emailContent =
      s"""$pattern
         |$beautifiedMessage
         |$pattern""".stripMargin

    // Wrap it in an HTML block for better email rendering
    val wrappedEmailContent =
      s"""<div style="font-family: Arial, sans-serif; color: red; border: 2px solid black; padding: 10px; text-align: center; width: 600px;">
         |<pre style="white-space: pre-wrap;">$emailContent</pre>
         |</div>""".stripMargin

    // Add the formatted message to AlertMessage
    AlertMessage.addMessage(wrappedEmailContent)
  }
}
