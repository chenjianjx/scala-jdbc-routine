package org.sjr.it.vendor.common.cases

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}
import org.sjr.RowHandler
import org.sjr.callable.{InOutParam, InParam, OutParam}
import org.sjr.it.vendor.common.support.{testJdbc, withConn}

import java.sql.Types


case class ForSpRecord(
                        float: Float,
                        floatOpt: Option[Float],
                        bigDecimal: BigDecimal,
                        bigDecimalOpt: Option[BigDecimal]
                      )


abstract class CallableStatementITCase extends VendorITCaseBase {

  protected def nameOfTableForStoredProcedure: String

  protected def ddlOfTableForStoredProcedure: String

  protected def ddlOfStoredProcedureWithoutResults: String

  protected def ddlOfStoredProcedureWithResults: String

  protected def sqlToCallStoredProcedureWithoutResults: String

  protected def sqlToCallStoredProcedureWithResults: String

  protected def forSpRecordRowHandler: RowHandler[ForSpRecord]

  override protected def perClassInit(): Unit = {
    withConn { conn =>
      testJdbc.execute(conn, ddlOfTableForStoredProcedure)
      testJdbc.execute(conn, ddlOfStoredProcedureWithoutResults)
      testJdbc.execute(conn, ddlOfStoredProcedureWithResults)
      ()
    }

  }


  @BeforeEach
  def beforeEach(): Unit = {
    withConn { conn =>
      testJdbc.update(conn, s"DELETE FROM $nameOfTableForStoredProcedure")
      ()
    }
  }


  @Test
  def callToUpdate_fillAllParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callToUpdate(sqlToCallStoredProcedureWithoutResults,
        InParam(1.0F),
        InParam(Some(2.0F)),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, Some(200.0F)),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )
      assertEquals(Map(3 -> BigDecimal(200), 4 -> BigDecimal(400), 5 -> 2.0F, 6 -> 4.0F), result.outValues)
    }
  }

  @Test
  def callToUpdate_fillRequiredParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callToUpdate(sqlToCallStoredProcedureWithoutResults,
        InParam(1.0F),
        InParam(None),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, None),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )
      assertEquals(Map(3 -> BigDecimal(200), 5 -> 2.0F), result.outValues)
    }
  }

  @Test
  def callForSeq(): Unit = {
    withConn { implicit conn =>

      jdbcRoutine.callToUpdate(sqlToCallStoredProcedureWithoutResults,
        InParam(1.0F),
        InParam(Some(2.0F)),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, Some(200.0F)),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      val result = jdbcRoutine.callForSeq(sqlToCallStoredProcedureWithResults, forSpRecordRowHandler)
      assertEquals(Seq(ForSpRecord(float = 1.0F, floatOpt = Some(2.0F), bigDecimal = BigDecimal(100.0F), bigDecimalOpt = Some(BigDecimal(200.0F)))), result.records)
    }
  }


  @Test
  def callToUpdate_againstSpWithResults(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.callToUpdate(sqlToCallStoredProcedureWithResults)
      ()
    }
  }

  @Test
  def callForSeq_againstSpWithoutResults(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callForSeq(sqlToCallStoredProcedureWithoutResults, forSpRecordRowHandler,
        InParam(1.0F),
        InParam(Some(2.0F)),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, Some(200.0F)),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(0, result.records.size)
      assertEquals(Map(3 -> BigDecimal(200), 4 -> BigDecimal(400), 5 -> 2.0F, 6 -> 4.0F), result.outValues)
    }
  }

}
