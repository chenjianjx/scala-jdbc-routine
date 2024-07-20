package org.sjr.it

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.sjr.JdbcRoutine


class CoverageRemedyITCase {

  implicit val jdbcUrl:String = createH2Db(this.getClass)

  private val jdbcRoutine = new JdbcRoutine

  @Test
  def getArrayFromMysqlResultSet(): Unit = {
    withH2Conn { implicit conn =>
      val sql = "SELECT ARRAY[1, 2, 3] as arr"
      assertEquals(Some(Seq(1, 2, 3)), jdbcRoutine.queryForSingle(sql, row => sqlArrayToSeq(row.getArray(1))))
      assertEquals(Some(Seq(1, 2, 3)), jdbcRoutine.queryForSingle(sql, row => sqlArrayToSeq(row.getArray("arr"))))
    }
  }

}
