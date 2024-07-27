package org.sjr.it.vendor.oracle.cases

import org.apache.commons.dbutils.handlers.MapListHandler
import org.sjr.it.vendor.common.cases.{CallableStatementITCase, ForSpRecord}
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory, testJdbc, withConn}
import org.sjr.it.vendor.oracle.support.createOracleContainer
import org.sjr.{RowHandler, WrappedResultSet}

import scala.jdk.CollectionConverters.CollectionHasAsScala

class OracleCallableStatementITCase extends CallableStatementITCase {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createOracleContainer())

  override protected def nameOfTableForStoredProcedure: String = "for_sp"

  override protected def ddlOfTableForStoredProcedure: String =
    """
      |CREATE TABLE for_sp (
      |    float_value NUMBER(19,4) NOT NULL,
      |    float_opt NUMBER(19,4),
      |    big_decimal NUMBER(38, 2) NOT NULL,
      |    big_decimal_opt NUMBER(38, 2)
      |)
      |""".stripMargin


  override protected def ddlOfStoredProcedureWithoutResults: String =
    """
      |CREATE PROCEDURE insert_sp(
      |    float_in IN NUMBER,
      |    float_in_opt IN NUMBER,
      |    big_decimal_twice_inout IN OUT NUMBER,
      |    big_decimal_twice_inout_opt IN OUT NUMBER,
      |    float_twice_out OUT NUMBER,
      |    float_twice_out_opt OUT NUMBER
      |)
      |IS
      |BEGIN
      |    INSERT INTO for_sp (float_value, float_opt, big_decimal, big_decimal_opt)
      |    VALUES (float_in, float_in_opt, big_decimal_twice_inout, big_decimal_twice_inout_opt);
      |
      |    big_decimal_twice_inout := big_decimal_twice_inout * 2;
      |    big_decimal_twice_inout_opt := CASE
      |                                      WHEN big_decimal_twice_inout_opt IS NULL THEN NULL
      |                                      ELSE big_decimal_twice_inout_opt * 2
      |                                   END;
      |    float_twice_out := float_in * 2;
      |    float_twice_out_opt := CASE
      |                              WHEN float_in_opt IS NULL THEN NULL
      |                              ELSE float_in_opt * 2
      |                           END;
      |
      |END insert_sp;
      |""".stripMargin


  override protected def ddlOfStoredProcedureWithResults: String =
    s"""
       |CREATE PROCEDURE query_sp
       |IS
       | row_cursor SYS_REFCURSOR;
       | count_cursor SYS_REFCURSOR;
       |BEGIN
       | OPEN row_cursor FOR SELECT * FROM for_sp;
       | DBMS_SQL.RETURN_RESULT(row_cursor);
       |
       | OPEN count_cursor FOR SELECT count(*) FROM for_sp;
       | DBMS_SQL.RETURN_RESULT(count_cursor);
       |END query_sp;
       |""".stripMargin


  override protected def perClassInit(): Unit = {
    super.perClassInit()
    withConn { conn =>
      val ddlErrors = testJdbc.query(conn, "select * from ALL_ERRORS where owner = 'TEST' order by NAME, SEQUENCE", new MapListHandler()).asScala.toSeq
      if(ddlErrors.size > 0){
        ddlErrors.foreach(System.err.println(_))
        throw new IllegalStateException("DDL execution are not successful.")
      }
      ()
    }
  }

  override protected def sqlToCallStoredProcedureWithoutResults: String = "{CALL insert_sp (?, ?, ?, ?, ?, ?)}"

  override protected def sqlToCallStoredProcedureWithResults: String = "{CALL query_sp()}"

  override protected def forSpRecordRowHandler: RowHandler[ForSpRecord] = new OracleForSpRecordRowHandler

  private class OracleForSpRecordRowHandler extends RowHandler[ForSpRecord] {
    override def handle(rs: WrappedResultSet): ForSpRecord = ForSpRecord(
      float = rs.getFloat("FLOAT_VALUE"),
      floatOpt = rs.getFloatOpt("FLOAT_OPT"),
      bigDecimal = rs.getBigDecimal("BIG_DECIMAL"),
      bigDecimalOpt = rs.getBigDecimalOpt("BIG_DECIMAL_OPT")
    )
  }

}
