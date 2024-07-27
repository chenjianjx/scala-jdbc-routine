package org.sjr

import java.sql.PreparedStatement

/**
 * Use this when [[PreparedStatement.setObject()]] is not good enough for your jdbc driver, i.e. `stmt.setXxx()` is required
 */
trait PreparedStatementSetterParam {
  /**
   * Tip: Can leverage [[org.sjr.PlainTypeConvert.scalaValueToJavaValueForJdbc()]] to fill in a statement with scala values
   */
  def doSet(stmt: PreparedStatement, index: Int): Unit
}