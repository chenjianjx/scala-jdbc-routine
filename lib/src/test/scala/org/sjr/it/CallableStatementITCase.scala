package org.sjr.it

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test}
import org.sjr.callable.{InOutParam, InParam, OutParam}
import org.sjr.{JdbcRoutine, RowHandler, WrappedResultSet}
import org.testcontainers.containers.MySQLContainer

import java.sql.Types


private case class ForSpRecord(
                                float: Float,
                                floatOpt: Option[Float],
                                bigDecimal: BigDecimal,
                                bigDecimalOpt: Option[BigDecimal]
                              )

object CallableStatementITCase {

  private implicit val testContainer: MySQLContainer[Nothing] = createTestContainer()

  private val TableForStoredProcedure =
    s"""
       |CREATE TABLE for_sp (
       |    float_value FLOAT NOT NULL,
       |    float_opt FLOAT,
       |    big_decimal DECIMAL(10, 2) NOT NULL,
       |    big_decimal_opt DECIMAL(10, 2)
       |);
       |""".stripMargin

  private val StoredProcedureWithoutResults =
    s"""
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

  private val StoredProcedureWithResults =
    s"""
       |${StoredProcedureWithoutResults.replace("insert_sp", "insert_sp_and_return_rows").replace("END", "")}
       |
       |  SELECT * FROM for_sp;
       |
       |  SELECT COUNT(*) as c FROM for_sp;
       |
       |END
       |""".stripMargin

  @BeforeAll
  def beforeAll(): Unit = {
    testContainer.start()

    withConn { conn =>
      testJdbc.execute(conn, TableForStoredProcedure)
      testJdbc.execute(conn, StoredProcedureWithoutResults)
      testJdbc.execute(conn, StoredProcedureWithResults)
      ()
    }
  }

  @AfterAll
  def afterAll(): Unit = {
    testContainer.close()
  }
}

class CallableStatementITCase {

  import CallableStatementITCase.testContainer

  private val jdbcRoutine = new JdbcRoutine

  @BeforeEach
  def beforeEach(): Unit = {
    withConn { conn =>
      testJdbc.update(conn, "DELETE FROM for_sp")
      ()
    }
  }


  @Test
  def callToUpdate_fillAllParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callToUpdate("CALL insert_sp (?, ?, ?, ?, ?, ?)",
        InParam(1.0F),
        InParam(Some(2.0F)),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, Some(200.0F)),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(1, result.updateCount)
      assertEquals(Map(3 -> BigDecimal(200), 4 -> BigDecimal(400), 5 -> 2.0F, 6 -> 4.0F), result.outValues)
    }
  }

  @Test
  def callToUpdate_fillRequiredParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callToUpdate("CALL insert_sp (?, ?, ?, ?, ?, ?)",
        InParam(1.0F),
        InParam(None),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, None),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(1, result.updateCount)
      assertEquals(Map(3 -> BigDecimal(200), 5 -> 2.0F), result.outValues)
    }
  }

  @Test
  def callForSeq_fillAllParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callForSeq("CALL insert_sp_and_return_rows (?, ?, ?, ?, ?, ?)", new ForSqRecordRowHandler,
        InParam(1.0F),
        InParam(Some(2.0F)),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, Some(200.0F)),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(1, result.records.size)
      assertEquals(ForSpRecord(
        float = 1.0F,
        floatOpt = Some(2.0F),
        bigDecimal = BigDecimal(100.0F),
        bigDecimalOpt = Some(BigDecimal(200.0F))), result.records(0))
      assertEquals(Map(3 -> BigDecimal(200), 4 -> BigDecimal(400), 5 -> 2.0F, 6 -> 4.0F), result.outValues)
    }
  }


  @Test
  def callForSeq_fillRequiredParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callForSeq("CALL insert_sp_and_return_rows (?, ?, ?, ?, ?, ?)", new ForSqRecordRowHandler,
        InParam(1.0F),
        InParam(None),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, None),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(1, result.records.size)
      assertEquals(ForSpRecord(
        float = 1.0F,
        floatOpt = None,
        bigDecimal = BigDecimal(100.0F),
        bigDecimalOpt = None), result.records(0))
      assertEquals(Map(3 -> BigDecimal(200), 5 -> 2.0F), result.outValues)
    }
  }


  @Test
  def callToUpdate_againstSpWithResults(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callToUpdate("CALL insert_sp_and_return_rows (?, ?, ?, ?, ?, ?)",
        InParam(1.0F),
        InParam(Some(2.0F)),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, Some(200.0F)),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(-1, result.updateCount)
      assertEquals(Map(3 -> BigDecimal(200), 4 -> BigDecimal(400), 5 -> 2.0F, 6 -> 4.0F), result.outValues)
    }
  }

  @Test
  def callForSeq_againstSpWithoutResults(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callForSeq("CALL insert_sp (?, ?, ?, ?, ?, ?)", new ForSqRecordRowHandler,
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


  private class ForSqRecordRowHandler extends RowHandler[ForSpRecord] {
    override def handle(rs: WrappedResultSet): ForSpRecord = ForSpRecord(
      float = rs.getFloat("FLOAT_VALUE"),
      floatOpt = rs.getFloatOpt("FLOAT_OPT"),
      bigDecimal = rs.getBigDecimal("BIG_DECIMAL"),
      bigDecimalOpt = rs.getBigDecimalOpt("BIG_DECIMAL_OPT")
    )
  }


}
