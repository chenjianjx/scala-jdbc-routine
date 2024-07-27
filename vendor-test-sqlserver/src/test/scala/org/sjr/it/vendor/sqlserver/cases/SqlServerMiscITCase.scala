package org.sjr.it.vendor.sqlserver.cases

import org.sjr.it.vendor.common.cases.MiscITCase
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory}
import org.sjr.it.vendor.sqlserver.support.createSqlServerContainer
import org.sjr.{GeneratedKeysHandler, WrappedResultSet}

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

}
