package scala.spark.validation.framework.alert

case class AlertMessage(message: List[String])

object AlertMessage {
  // Using synchronized ListBuffer for thread safety
  private val messages = scala.collection.mutable.ListBuffer[String]()

  /**
   * Adds a message to the list.
   *
   * @param message The message to be added.
   */
  def addMessage(message: String): Unit = {
    messages += message
  }

  /**
   * Retrieves all messages as a combined string.
   *
   * @return A single string containing all messages.
   */
  def getCombinedMessage: String = {
    messages.mkString
  }


  /**
   * Clears all stored messages.
   */
  def clearMessage(): Unit = {
    messages.clear()
  }
}
