package org.sjr.it.vendor.oracle.cases

import org.junit.jupiter.api.Test
import org.sjr.it.vendor.common.cases.MiscITCase
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory, withConn}
import org.sjr.it.vendor.oracle.support.createOracleContainer
import org.sjr.{GeneratedKeysHandler, PreparedStatementSetterParam, WrappedResultSet}

import java.sql.PreparedStatement

class OracleMiscITCase extends MiscITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createOracleContainer())

  override protected def nameOfTableWithAutoKey: String = "auto_key_record"

  override protected def ddlOfTableWithAutoKey: Seq[String] =
    Seq(
      """
        | CREATE SEQUENCE auto_key_seq start with 1 increment by 1 nocache
      """.stripMargin,

      """
        | CREATE TABLE auto_key_record (
        |   id NUMBER(19) PRIMARY KEY,
        |   int_value NUMBER(10) NOT NULL
        |   )
      """.stripMargin
    )

  override protected def insertIntoTableWithAutoKey: String = s"insert into $nameOfTableWithAutoKey(id, int_value) values(auto_key_seq.NEXTVAL, ?)"

  override protected def returnedColumnsToGetKey: Option[Array[String]] = Some(Array("id"))

  override protected def generatedKeysHandler: GeneratedKeysHandler[Long] = new OracleGeneratedKeysHandler

  private class OracleGeneratedKeysHandler extends GeneratedKeysHandler[Long] {
    override def handle(resultSet: WrappedResultSet): Option[Long] =
      if (resultSet.next()) {
        Some(resultSet.getScalaLong(1))
      } else {
        None
      }
  }

  @Test
  def complementTestCoverage_getAutoKey(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.updateAndGetGeneratedKeys[Long](insertIntoTableWithAutoKey, _ => None, 123)
      ()
    }
  }

  @Test
  def complementTestCoverage_useOptionalSetterParam(): Unit = {
    withConn {
      implicit conn =>

        jdbcRoutine.update(insertIntoTableWithAutoKey, Some(new PreparedStatementSetterParam {
          override def doSet(stmt: PreparedStatement, index: Int): Unit = stmt.setInt(index, 222)
        }))

        ()
    }
  }


}
