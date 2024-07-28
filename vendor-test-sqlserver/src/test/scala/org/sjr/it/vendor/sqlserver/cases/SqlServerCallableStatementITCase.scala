package org.sjr.it.vendor.sqlserver.cases

import org.sjr.it.vendor.common.cases.{CallableStatementITCase, ForSpRecord}
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory}
import org.sjr.it.vendor.sqlserver.support.createSqlServerContainer
import org.sjr.{RowHandler, WrappedResultSet}

class SqlServerCallableStatementITCase extends CallableStatementITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createSqlServerContainer())

  override protected def nameOfTableForStoredProcedure: String = "for_sp"

  override protected def ddlOfTableForStoredProcedure: String =
    """
      |CREATE TABLE for_sp (
      |    float_value REAL NOT NULL,
      |    float_opt REAL,
      |    big_decimal DECIMAL(38, 2) NOT NULL,
      |    big_decimal_opt DECIMAL(38, 2)
      |)
      |""".stripMargin


  override protected def ddlOfStoredProcedureWithoutResults: String =
    """
      |CREATE PROCEDURE insert_sp
      |    @float_in REAL,
      |    @float_in_opt REAL,
      |    @big_decimal_twice_inout DECIMAL(38, 2) OUTPUT,
      |    @big_decimal_twice_inout_opt DECIMAL(38, 2) OUTPUT,
      |    @float_twice_out REAL OUT,
      |    @float_twice_out_opt REAL OUT
      |AS
      |BEGIN
      |    INSERT INTO for_sp (float_value, float_opt, big_decimal, big_decimal_opt)
      |    VALUES (@float_in, @float_in_opt, @big_decimal_twice_inout, @big_decimal_twice_inout_opt);
      |
      |    SET @big_decimal_twice_inout = @big_decimal_twice_inout * 2;
      |    SET @big_decimal_twice_inout_opt = CASE
      |                                          WHEN @big_decimal_twice_inout_opt IS NULL THEN NULL
      |                                          ELSE @big_decimal_twice_inout_opt * 2
      |                                       END;
      |
      |    SET @float_twice_out = @float_in * 2;
      |    SET @float_twice_out_opt = CASE
      |                                  WHEN @float_in_opt IS NULL THEN NULL
      |                                  ELSE @float_in_opt * 2
      |                               END;
      |END
      |""".stripMargin


  override protected def ddlOfStoredProcedureWithResults: String =
    s"""
       |CREATE PROCEDURE query_sp
       |AS
       |BEGIN
       |    SELECT * FROM for_sp;
       |
       |    SELECT COUNT(*) AS row_count FROM for_sp;
       |END
       |
       |""".stripMargin

  override protected def sqlToCallStoredProcedureWithoutResults: String = "{CALL insert_sp (?, ?, ?, ?, ?, ?)}"

  override protected def sqlToCallStoredProcedureWithResults: String = "{CALL query_sp()}"

  override protected def forSpRecordRowHandler: RowHandler[ForSpRecord] = new SqlServerForSpRecordRowHandler

  private class SqlServerForSpRecordRowHandler extends RowHandler[ForSpRecord] {
    override def handle(rs: WrappedResultSet): ForSpRecord = ForSpRecord(
      float = rs.getScalaFloat("FLOAT_VALUE"),
      floatOpt = rs.getScalaFloatOpt("FLOAT_OPT"),
      bigDecimal = rs.getBigDecimal("BIG_DECIMAL"),
      bigDecimalOpt = rs.getBigDecimalOpt("BIG_DECIMAL_OPT")
    )
  }

}
