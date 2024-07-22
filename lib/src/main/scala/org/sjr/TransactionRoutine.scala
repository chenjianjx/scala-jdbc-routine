package org.sjr

import java.sql.{Connection, SQLException}

/**
 * Note: You can use this class without [[JdbcRoutine]]. You can also use [[JdbcRoutine]] without this class
 */
object TransactionRoutine {

  @throws[SQLException]
  def transaction[T](action: => T)(implicit conn: Connection): T = {
    try {
      conn.setAutoCommit(false)
      val result = action
      conn.commit()
      result
    } catch {
      case e: SQLException =>
        conn.rollback()
        throw e
    } finally {
      conn.setAutoCommit(true)
    }
  }
}
