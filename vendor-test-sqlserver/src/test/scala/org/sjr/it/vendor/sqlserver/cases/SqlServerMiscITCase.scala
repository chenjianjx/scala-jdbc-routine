package org.sjr.it.vendor.sqlserver.cases

import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.sjr.it.vendor.common.cases.MiscITCase
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory, withConn}
import org.sjr.it.vendor.sqlserver.support.createSqlServerContainer
import org.sjr.{GeneratedKeysHandler, PreparedStatementSetterParam, WrappedResultSet}

import java.sql.{PreparedStatement, SQLException}

class SqlServerMiscITCase extends MiscITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createSqlServerContainer())

  override protected def nameOfTableWithAutoKey: String = "auto_key_record"

  override protected def ddlOfTableWithAutoKey: Seq[String] = Seq(
    """
      |CREATE TABLE auto_key_record (
      |  id BIGINT IDENTITY(1,1) PRIMARY KEY,
      |  int_value INT NOT NULL
      |  )
      """.stripMargin)

  override protected def insertIntoTableWithAutoKey: String = s"insert into $nameOfTableWithAutoKey(int_value) values(?)"

  override protected def generatedKeysHandler: GeneratedKeysHandler[Long] = new SqlServerGeneratedKeysHandler

  private class SqlServerGeneratedKeysHandler extends GeneratedKeysHandler[Long] {
    override def handle(resultSet: WrappedResultSet): Option[Long] =
      if (resultSet.next()) {
        resultSet.getScalaLongOpt(1)
      } else {
        None
      }
  }

  @Test
  def complementTestCoverage_getAutoKeyFromReturnedColumns(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.updateAndGetGeneratedKeysFromReturnedColumns[Long](insertIntoTableWithAutoKey, Array("whatever"), generatedKeysHandler, 123)
      ()
    }
  }


  @Test
  def complementTestCoverage_unsupportedTypes(): Unit = {

    withConn { implicit conn =>
      val sql = "SELECT 'abc' AS value"

      //unsupported by the jdbc driver
      assertThrows(classOf[SQLException], () => jdbcRoutine.queryForSingle(sql, row => row.getArray("value")).asInstanceOf[Unit])
      assertThrows(classOf[SQLException], () => jdbcRoutine.queryForSingle(sql, row => row.getArray(1)).asInstanceOf[Unit])

      //unsupported by the jdbc driver
      assertThrows(classOf[SQLException], () => jdbcRoutine.queryForSingle(sql, row => row.getURL("value")).asInstanceOf[Unit])
      assertThrows(classOf[SQLException], () => jdbcRoutine.queryForSingle(sql, row => row.getURL(1)).asInstanceOf[Unit])

      ()
    }

  }

  @Test
  def complementTestCoverage_useOptionSetterParam(): Unit = {
    withConn {
      implicit conn =>
        jdbcRoutine.update(insertIntoTableWithAutoKey, Some(new PreparedStatementSetterParam {
          override def doSet(stmt: PreparedStatement, index: Int): Unit = stmt.setInt(index, 222)
        }))
        ()
    }
  }

}
