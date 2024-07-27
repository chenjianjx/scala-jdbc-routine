package org.sjr.it.vendor.postgresql.cases

import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.sjr.it.vendor.common.cases.MiscITCase
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory, withConn}
import org.sjr.it.vendor.postgresql.support.createPgContainer
import org.sjr.{GeneratedKeysHandler, WrappedResultSet}

import java.sql.{SQLException, SQLFeatureNotSupportedException}

class PgMiscITCase extends MiscITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createPgContainer())

  override protected def nameOfTableWithAutoKey: String = "auto_key_record"

  override protected def ddlOfTableWithAutoKey: Seq[String] = Seq(
    """
      |CREATE TABLE auto_key_record (
      |  id BIGSERIAL PRIMARY KEY,
      |  int_value INT NOT NULL
      |  )
      """.stripMargin)

  override protected def insertIntoTableWithAutoKey: String = s"insert into $nameOfTableWithAutoKey(int_value) values(?)"

  override protected def generatedKeysHandler: GeneratedKeysHandler[Long] = new PgGeneratedKeysHandler

  private class PgGeneratedKeysHandler extends GeneratedKeysHandler[Long] {
    override def handle(resultSet: WrappedResultSet): Option[Long] = if (resultSet.next()) {
      Some(resultSet.getScalaLong(1))
    } else {
      None
    }
  }


  @Test
  def complementTestCoverage_unsupportedTypes(): Unit = {

    withConn { implicit conn =>

      val sql = "SELECT 'abc' AS value"

      //Not supported by postgres's jdbc driver
      assertThrows(classOf[SQLException], () => jdbcRoutine.queryForSingle(sql, row => row.getClob("value")).asInstanceOf[Unit])
      assertThrows(classOf[SQLException], () => jdbcRoutine.queryForSingle(sql, row => row.getClob(1)).asInstanceOf[Unit])

      //Not supported by postgres's jdbc driver
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getNCharacterStream("value")).asInstanceOf[Unit])
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getNCharacterStream(1)).asInstanceOf[Unit])

      //Not supported by postgres's jdbc driver
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getNClob("value")).asInstanceOf[Unit])
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getNClob(1)).asInstanceOf[Unit])

      //Not supported by postgres's jdbc driver
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getNString("value")).asInstanceOf[Unit])
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getNString(1)).asInstanceOf[Unit])

      //Not supported by postgres's jdbc driver
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getURL("value")).asInstanceOf[Unit])
      assertThrows(classOf[SQLFeatureNotSupportedException], () => jdbcRoutine.queryForSingle(sql, row => row.getURL(1)).asInstanceOf[Unit])

      ()
    }

  }

  @Test
  def complementTestCoverage_getAutoKeyFromReturnedColumns(): Unit = {

    withConn { implicit conn =>
      jdbcRoutine.updateAndGetGeneratedKeysFromReturnedColumns[Long](insertIntoTableWithAutoKey, Array("id"), generatedKeysHandler, 123)
      ()
    }

  }

}
