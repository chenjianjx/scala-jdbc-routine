package org.sjr.it.vendor.postgresql.cases

import org.sjr.it.vendor.common.cases.MiscITCase
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory}
import org.sjr.it.vendor.mysql.support.createMySQLContainer
import org.sjr.it.vendor.postgresql.support.createPgContainer
import org.sjr.{GeneratedKeysHandler, WrappedResultSet}

class PgMiscITCase extends MiscITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createPgContainer())

  override protected def nameOfTableWithAutoKey: String = "auto_key_record"

  override protected def ddlOfTableWithAutoKey: String =
    """
      |CREATE TABLE auto_key_record (
      |  id BIGSERIAL PRIMARY KEY,
      |  int_value INT NOT NULL
      |  )
      """.stripMargin


  override protected def generatedKeysHandler: GeneratedKeysHandler[Long] = new MySqlGeneratedKeysHandler

  private class MySqlGeneratedKeysHandler extends GeneratedKeysHandler[Long] {
    override def handle(resultSet: WrappedResultSet): Option[Long] = if (resultSet.next()) {
      Some(resultSet.getLong(1))
    } else {
      None
    }
  }


}
