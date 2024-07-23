package org.sjr.it.vendor.postgresql.cases

import org.sjr.it.vendor.common.cases.{CallableStatementITCase, ForSpRecord}
import org.sjr.it.vendor.common.support
import org.sjr.it.vendor.common.support.TestContainerConnFactory
import org.sjr.it.vendor.postgresql.support.createPgContainer
import org.sjr.{RowHandler, WrappedResultSet}

class PgCallableStatementITCase extends CallableStatementITCase {

  override protected def getConnFactory(): support.ConnFactory = new TestContainerConnFactory(createPgContainer())

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
      |    float_in FLOAT,
      |    float_in_opt FLOAT,
      |    INOUT big_decimal_twice_inout DECIMAL(10, 2),
      |    INOUT big_decimal_twice_inout_opt DECIMAL(10, 2),
      |    OUT float_twice_out FLOAT,
      |    OUT float_twice_out_opt FLOAT
      |) AS $$
      |BEGIN
      |    INSERT INTO for_sp (float_value, float_opt, big_decimal, big_decimal_opt)
      |    VALUES (float_in, float_in_opt, big_decimal_twice_inout, big_decimal_twice_inout_opt);
      |
      |    big_decimal_twice_inout := big_decimal_twice_inout * 2;
      |    big_decimal_twice_inout_opt := big_decimal_twice_inout_opt * 2;
      |    float_twice_out := float_in * 2;
      |    float_twice_out_opt := float_in_opt * 2;
      |
      |END;
      |$$ LANGUAGE plpgsql;
      |""".stripMargin


  override protected def ddlOfStoredProcedureWithResults: String =
    """
      |CREATE FUNCTION query_sp() RETURNS for_sp
      |AS $$ SELECT * FROM for_sp $$
      |LANGUAGE SQL;
      |""".stripMargin

  override protected def sqlToCallStoredProcedureWithoutResults: String = "call insert_sp(?::FLOAT, ?::FLOAT, ?::DECIMAL, ?::DECIMAL, ?::FLOAT, ?::FLOAT)"

  override protected def sqlToCallStoredProcedureWithResults: String = "{call query_sp()}"

  override protected def forSpRecordRowHandler: RowHandler[ForSpRecord] = new PgForSpRecordRowHandler

  private class PgForSpRecordRowHandler extends RowHandler[ForSpRecord] {
    override def handle(rs: WrappedResultSet): ForSpRecord = ForSpRecord(
      float = rs.getFloat("FLOAT_VALUE"),
      floatOpt = rs.getFloatOpt("FLOAT_OPT"),
      bigDecimal = rs.getBigDecimal("BIG_DECIMAL"),
      bigDecimalOpt = rs.getBigDecimalOpt("BIG_DECIMAL_OPT")
    )
  }

}
