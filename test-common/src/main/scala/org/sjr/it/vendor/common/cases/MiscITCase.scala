package org.sjr.it.vendor.common.cases

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.sjr.GeneratedKeysHandler
import org.sjr.it.vendor.common.support.{testJdbc, withConn}

abstract class MiscITCase extends VendorITCaseBase {

  protected def nameOfTableWithAutoKey: String

  protected def ddlOfTableWithAutoKey: Seq[String]

  protected def insertIntoTableWithAutoKey: String

  protected def returnedColumnsToGetKey: Option[Array[String]] = None

  protected def generatedKeysHandler: GeneratedKeysHandler[Long] // Use a generic type instead of Long, if needed

  override protected def perClassInit(): Unit = {
    withConn { conn =>
      ddlOfTableWithAutoKey.foreach(testJdbc.execute(conn, _))
      ()
    }
  }


  @Test
  def execute(): Unit = {
    val tableName = s"random_table_${System.currentTimeMillis()}"
    val ddl =
      s"""
         |CREATE TABLE $tableName (
         |  random_column VARCHAR(10)
         |)""".stripMargin

    withConn { implicit conn =>
      jdbcRoutine.execute(ddl)
      testJdbc.execute(conn, s"Truncate Table $tableName") //should succeed
      ()
    }
  }


  @Test
  def insertAndGetGeneratedKeys(): Unit = {

    withConn { implicit conn =>

      val (affectedRows, keys) = if (returnedColumnsToGetKey.isDefined) {
        jdbcRoutine.updateAndGetGeneratedKeysFromReturnedColumns[Long](insertIntoTableWithAutoKey, returnedColumnsToGetKey.get, generatedKeysHandler, 123)
      } else {
        jdbcRoutine.updateAndGetGeneratedKeys[Long](insertIntoTableWithAutoKey, generatedKeysHandler, 123)
      }

      assertEquals(1, affectedRows)
      assertEquals(Some(1L), keys)
    }
  }


  @Test
  def updateAndGetGeneratedKeys_noGeneration(): Unit = {
    withConn { implicit conn =>
      val sql = s"UPDATE $nameOfTableWithAutoKey SET int_value = 999 where id = ? "
      val (affectedRows, keys) = if (returnedColumnsToGetKey.isDefined) {
        jdbcRoutine.updateAndGetGeneratedKeysFromReturnedColumns[Long](sql, returnedColumnsToGetKey.get, generatedKeysHandler, -1)
      } else {
        jdbcRoutine.updateAndGetGeneratedKeys[Long](sql, generatedKeysHandler, -1)
      }

      assertEquals(0, affectedRows)
      assertEquals(None, keys)
    }
  }


  @Test
  def getObjectFromResultSet(): Unit = {
    withConn { implicit conn =>

      val sql = "SELECT 'abc' as value"

      val expected = Some("abc")

      assertEquals(expected, jdbcRoutine.queryForSingle(sql, row => row.getObject(1)))
      assertEquals(expected, jdbcRoutine.queryForSingle(sql, row => row.getObject("value")))

      assertEquals(expected, jdbcRoutine.queryForSingle(sql, row => row.getObject(1, classOf[String])))
      assertEquals(expected, jdbcRoutine.queryForSingle(sql, row => row.getObject("value", classOf[String])))
    }
  }


}
