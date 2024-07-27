package org.sjr.it.vendor.mysql.cases

import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.sjr.it.vendor.common.cases.MiscITCase
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory, withConn}
import org.sjr.it.vendor.mysql.support.createMySQLContainer
import org.sjr.{GeneratedKeysHandler, PreparedStatementSetterParam, WrappedResultSet}

import java.sql.{PreparedStatement, SQLFeatureNotSupportedException}

class MySqlMiscITCase extends MiscITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createMySQLContainer())

  override protected def nameOfTableWithAutoKey: String = "auto_key_record"

  override protected def ddlOfTableWithAutoKey: Seq[String] = Seq(
    """
      |CREATE TABLE auto_key_record (
      |  id BIGINT AUTO_INCREMENT PRIMARY KEY,
      |  int_value INT NOT NULL
      |  )
      """.stripMargin)

  override protected def insertIntoTableWithAutoKey: String = s"insert into $nameOfTableWithAutoKey(int_value) values(?)"

  override protected def generatedKeysHandler: GeneratedKeysHandler[Long] = new MySqlGeneratedKeysHandler

  private class MySqlGeneratedKeysHandler extends GeneratedKeysHandler[Long] {
    override def handle(resultSet: WrappedResultSet): Option[Long] = if (resultSet.next()) {
      Some(resultSet.getScalaLong(1))
    } else {
      None
    }
  }

  //getArray() is not supported by mysql's jdbc driver.
  @Test
  def complementTestCoverage_getArray(): Unit = {

    withConn { implicit conn =>
      val sql = "SELECT 'abc' AS value"
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getArray("value")).asInstanceOf[Unit])
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getArray(1)).asInstanceOf[Unit])
      ()
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
  def complementTestCoverage_useSetterParam(): Unit = {
    withConn {
      implicit conn =>
        jdbcRoutine.update(insertIntoTableWithAutoKey, new PreparedStatementSetterParam {
          override def doSet(stmt: PreparedStatement, index: Int): Unit = stmt.setInt(index, 111)
        })

        jdbcRoutine.update(insertIntoTableWithAutoKey, Some(new PreparedStatementSetterParam {
          override def doSet(stmt: PreparedStatement, index: Int): Unit = stmt.setInt(index, 222)
        }))

        ()
    }
  }

}
