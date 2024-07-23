package org.sjr.it.vendor.mysql.cases

import org.sjr.it.vendor.common.cases.{CallableStatementITCase, ForSpRecord}
import org.sjr.it.vendor.common.support
import org.sjr.it.vendor.common.support.TestContainerConnFactory
import org.sjr.it.vendor.mysql.support.createMySQLContainer
import org.sjr.{RowHandler, WrappedResultSet}

class MySqlCallableStatementITCase extends CallableStatementITCase {

  override protected def getConnFactory(): support.ConnFactory = new TestContainerConnFactory(createMySQLContainer())

  override protected def nameOfTableForStoredProcedure: String = "for_sp"

  override protected def ddlOfTableForStoredProcedure: String =
    """
      |CREATE TABLE for_sp (
      |    float_value FLOAT NOT NULL,
      |    float_opt FLOAT,
      |    big_decimal DECIMAL(10, 2) NOT NULL,
      |    big_decimal_opt DECIMAL(10, 2)
      |);
      |""".stripMargin


  override protected def ddlOfStoredProcedureWithoutResults: String =
    """
      |CREATE PROCEDURE insert_sp(
      |    IN float_in FLOAT,
      |    IN float_in_opt FLOAT,
      |    INOUT big_decimal_twice_inout DECIMAL(10, 2),
      |    INOUT big_decimal_twice_inout_opt DECIMAL(10, 2),
      |    OUT float_twice_out FLOAT,
      |    OUT float_twice_out_opt FLOAT
      |)
      |BEGIN
      |    INSERT INTO for_sp (float_value, float_opt, big_decimal, big_decimal_opt)
      |    VALUES (float_in, float_in_opt, big_decimal_twice_inout, big_decimal_twice_inout_opt);
      |
      |    SET big_decimal_twice_inout = big_decimal_twice_inout * 2;
      |    SET big_decimal_twice_inout_opt = IFNULL(big_decimal_twice_inout_opt * 2, NULL);
      |    SET float_twice_out = float_in * 2;
      |    SET float_twice_out_opt = IFNULL(float_in_opt * 2, NULL);
      |END
      |""".stripMargin


  override protected def ddlOfStoredProcedureWithResults: String =
    s"""
       |CREATE PROCEDURE query_sp()
       |BEGIN
       |  SELECT * FROM for_sp;
       |  SELECT COUNT(*) as c FROM for_sp;
       |END
       |""".stripMargin

  override protected def sqlToCallStoredProcedureWithoutResults: String = "CALL insert_sp (?, ?, ?, ?, ?, ?)"

  override protected def sqlToCallStoredProcedureWithResults: String = "CALL query_sp ()"

  override protected def forSpRecordRowHandler: RowHandler[ForSpRecord] = new MySqlForSpRecordRowHandler

  private class MySqlForSpRecordRowHandler extends RowHandler[ForSpRecord] {
    override def handle(rs: WrappedResultSet): ForSpRecord = ForSpRecord(
      float = rs.getFloat("FLOAT_VALUE"),
      floatOpt = rs.getFloatOpt("FLOAT_OPT"),
      bigDecimal = rs.getBigDecimal("BIG_DECIMAL"),
      bigDecimalOpt = rs.getBigDecimalOpt("BIG_DECIMAL_OPT")
    )
  }

}
