package org.sjr.it.vendor.oracle.cases

import org.sjr.it.vendor.common.cases.MiscITCase
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory}
import org.sjr.{GeneratedKeysHandler, WrappedResultSet}
import org.sjr.it.vendor.oracle.support.createOracleContainer

class OracleMiscITCase extends MiscITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createOracleContainer())

  override protected def nameOfTableWithAutoKey: String = "auto_key_record"

  override protected def ddlOfTableWithAutoKey: Seq[String] =
    Seq("""
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
    override def handle(resultSet: WrappedResultSet): Option[Long] = if (resultSet.next()) {
      Some(resultSet.getLong(1))
    } else {
      None
    }
  }


}
