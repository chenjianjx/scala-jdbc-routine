package org.sjr.it.vendor.mysql.cases

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.sjr.JdbcRoutine
import org.sjr.it.vendor.common.support.special.{createH2Db, withH2Conn}
import org.sjr.testutil.sqlArrayToSeq


class MySqlCoverageRemedyITCase {

  implicit val jdbcUrl:String = createH2Db(this.getClass)

  private val jdbcRoutine = new JdbcRoutine

  /**
   * mysql jdbc driver doesn't support [[java.sql.ResultSet#getArray]],  so use h2 here to support [[org.sjr.WrappedResultSet#getArray]] and increase test coverage
   */
  @Test
  def getArrayFromMysqlResultSet(): Unit = {
    withH2Conn { implicit conn =>
      val sql = "SELECT ARRAY[1, 2, 3] as arr"
      assertEquals(Some(Seq(1, 2, 3)), jdbcRoutine.queryForSingle(sql, row => sqlArrayToSeq[Int](row.getArray(1))))
      assertEquals(Some(Seq(1, 2, 3)), jdbcRoutine.queryForSingle(sql, row => sqlArrayToSeq[Int](row.getArray("arr"))))
    }
  }

}
