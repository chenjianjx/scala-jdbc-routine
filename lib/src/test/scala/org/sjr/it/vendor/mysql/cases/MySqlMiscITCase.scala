package org.sjr.it.vendor.mysql.cases

import org.sjr.it.vendor.common.cases.MiscITCase
import org.sjr.it.vendor.mysql.support.createMySQLContainer
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory}
import org.sjr.{GeneratedKeysHandler, WrappedResultSet}

class MySqlMiscITCase extends MiscITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createMySQLContainer())

  override protected def nameOfTableWithAutoKey: String = "auto_key_record"

  override protected def ddlOfTableWithAutoKey: String =
    """
      |CREATE TABLE auto_key_record (
      |  id BIGINT AUTO_INCREMENT PRIMARY KEY,
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
