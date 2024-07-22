package org.sjr.callable

sealed trait CallableStatementParam {
  def canOutput: Boolean
}

case class InParam(
                    /**
                     * Can be an Option
                     */
                    value: Any
                  ) extends CallableStatementParam {
  override def canOutput: Boolean = false
}

case class OutParam(sqlType: Int) extends CallableStatementParam {
  override def canOutput: Boolean = true
}

case class InOutParam(sqlType: Int,

                      /**
                       * Can be an Option
                       */
                      value: Any) extends CallableStatementParam {
  override def canOutput: Boolean = true
}