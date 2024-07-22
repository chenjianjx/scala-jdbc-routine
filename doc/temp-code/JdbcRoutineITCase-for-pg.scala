package org.sjr.it

import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.MapListHandler
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeAll, BeforeEach, Test}
import org.sjr.TransactionRoutine.transaction
import org.sjr.callable.{InOutParam, InParam, OutParam}
import org.sjr.it.JdbcRoutineITCase._
import org.sjr.testutil.extractFieldValues
import org.sjr.{JdbcRoutine, RowHandler, WrappedResultSet}
import org.testcontainers.containers.{MySQLContainer, PostgreSQLContainer}

import java.sql.{Connection, DatabaseMetaData, DriverManager, JDBCType, ResultSet, SQLException, SQLNonTransientException, Types}
import java.util
import scala.util.Using


case class SomeRecord(
                       id: Long,
                       someInt: Int,
                       optionalInt: Option[Int],
                       someBigInt: Long,
                       optionalBigInt: Option[Long],
                       someSmallInt: Short,
                       optionalSmallInt: Option[Short],
                       someFloat: Float,
                       optionalFloat: Option[Float],
                       someDouble: Double,
                       optionalDouble: Option[Double],
                       someDecimal: BigDecimal,
                       optionalDecimal: Option[BigDecimal],
                       someChar: String,
                       optionalChar: Option[String],
                       someVarchar: String,
                       optionalVarchar: Option[String],
                       someDate: java.sql.Date,
                       optionalDate: Option[java.sql.Date],
                       someTime: java.sql.Time,
                       optionalTime: Option[java.sql.Time],
                       someTimestamp: java.sql.Timestamp,
                       optionalTimestamp: Option[java.sql.Timestamp],
                       someBoolean: Boolean,
                       optionalBoolean: Option[Boolean],
                       someBinary: Array[Byte],
                       optionalBinary: Option[Array[Byte]]
                     )

case class ForSp(
                  someFloat: Float,
                  optionalFloat: Option[Float],
                  someDecimal: BigDecimal,
                  optionalDecimal: Option[BigDecimal]
                )

object JdbcRoutineITCase {
  private val PostgreSQLContainer = new PostgreSQLContainer("postgres")
  //TODO: remove this - val JdbcUrl = s"jdbc:h2:./build/db/${this.getClass.getSimpleName}-${System.currentTimeMillis()};DB_CLOSE_DELAY=-1;MODE=MySQL"

  val SomeRecordTable =
    """
      |CREATE TABLE some_record (
      |  id BIGINT PRIMARY KEY,
      |  some_int INT NOT NULL,
      |  optional_int INT,
      |  some_bigint BIGINT NOT NULL,
      |  optional_bigint BIGINT,
      |  some_smallint SMALLINT NOT NULL,
      |  optional_smallint SMALLINT,
      |  some_float FLOAT NOT NULL,
      |  optional_float FLOAT,
      |  some_double DOUBLE PRECISION NOT NULL,
      |  optional_double DOUBLE PRECISION,
      |  some_decimal DECIMAL(10, 2) NOT NULL,
      |  optional_decimal DECIMAL(10, 2),
      |  some_char CHAR(20) NOT NULL,
      |  optional_char CHAR(20),
      |  some_varchar VARCHAR(255) NOT NULL,
      |  optional_varchar VARCHAR(255),
      |  some_date DATE NOT NULL,
      |  optional_date DATE,
      |  some_time TIME NOT NULL,
      |  optional_time TIME,
      |  some_timestamp TIMESTAMP NOT NULL,
      |  optional_timestamp TIMESTAMP,
      |  some_boolean BOOLEAN NOT NULL,
      |  optional_boolean BOOLEAN,
      |  some_binary BYTEA NOT NULL,
      |  optional_binary BYTEA
      |)
      |""".stripMargin


  val ForStoredProcedureTable =
    s"""
       |CREATE TABLE for_sp (
       |    some_float FLOAT NOT NULL,
       |    optional_float FLOAT,
       |    some_decimal DECIMAL(10, 2) NOT NULL,
       |    optional_decimal DECIMAL(10, 2)
       |);
       |""".stripMargin

  val NoResultStoredProcedure =
    """
      |CREATE procedure insert_sp(
      |    IN some_float_in FLOAT,
      |    IN optional_float_in FLOAT,
      |    INOUT some_decimal_twice_inout DECIMAL(10, 2),
      |    INOUT optional_decimal_twice_inout DECIMAL(10, 2),
      |    OUT some_float_twice_out FLOAT,
      |    OUT optional_float_twice_out FLOAT
      |)
      |LANGUAGE plpgsql
      |AS
      |$$
      |BEGIN
      |    INSERT INTO for_sp (some_float, optional_float, some_decimal, optional_decimal)
      |    VALUES (some_float_in, optional_float_in, some_decimal_twice_inout, optional_decimal_twice_inout);
      |
      |    some_decimal_twice_inout := some_decimal_twice_inout * 2;
      |    optional_decimal_twice_inout := COALESCE(optional_decimal_twice_inout * 2, NULL);
      |    some_float_twice_out := some_float_in * 2;
      |    optional_float_twice_out := COALESCE(optional_float_in * 2, NULL);
      |END;$$
      |""".stripMargin

  val WithResultsStoredProcedure =
    s"""
       |${NoResultStoredProcedure.replace("insert_sp", "insert_and_return_rows_sp").replace("END;$$", "")}
       |
       |  SELECT * FROM for_sp;
       |
       |  --SELECT COUNT(*) as c FROM for_sp;
       |
       |END;$$$$
       |""".stripMargin

  @BeforeAll
  def beforeAll(): Unit = {
    PostgreSQLContainer.start()

    withConn { conn =>
      testJdbc.execute(conn, SomeRecordTable)
      testJdbc.execute(conn, ForStoredProcedureTable)
      testJdbc.execute(conn, NoResultStoredProcedure)
      testJdbc.execute(conn, WithResultsStoredProcedure)
      ()
    }

  }

  private def testJdbc = {
    new QueryRunner()
  }

  private def withConn[T](job: Connection => T): T = {
    Using.resource(getConnection()) { conn =>
      job(conn)
    }
  }


  private def getConnection() = {
    val conn = DriverManager.getConnection(PostgreSQLContainer.getJdbcUrl, PostgreSQLContainer.getUsername, PostgreSQLContainer.getPassword)
    //    val metaData: DatabaseMetaData = conn.getMetaData
    //
    //    // Query to get stored procedures
    //    val procedures: ResultSet = metaData.getProcedures(null, null, null)
    //
    //    while (procedures.next()) {
    //      val procedureName = procedures.getString("PROCEDURE_NAME")
    //      val procedureSchema = procedures.getString("PROCEDURE_SCHEM")
    //
    //      println(s"Schema: $procedureSchema, Procedure: $procedureName")
    //
    //      val parameters: ResultSet = metaData.getProcedureColumns(null, null, procedureName, null)
    //
    //      while (parameters.next()) {
    //        val parameterName = parameters.getString("COLUMN_NAME")
    //        val parameterType = parameters.getInt("DATA_TYPE")
    //        val parameterTypeName = parameters.getString("TYPE_NAME")
    //
    //        println(s"  Parameter: $parameterName, Type: $parameterType ($parameterTypeName)")
    //      }
    //    }

    conn
  }

}

class JdbcRoutineITCase {


  val jdbcRoutine = new JdbcRoutine


  val InsertSomeRecordSql =
    """
      |INSERT INTO some_record (
      |    id,
      |    some_int,
      |    optional_int,
      |    some_bigint,
      |    optional_bigint,
      |    some_smallint,
      |    optional_smallint,
      |    some_float,
      |    optional_float,
      |    some_double,
      |    optional_double,
      |    some_decimal,
      |    optional_decimal,
      |    some_char,
      |    optional_char,
      |    some_varchar,
      |    optional_varchar,
      |    some_date,
      |    optional_date,
      |    some_time,
      |    optional_time,
      |    some_timestamp,
      |    optional_timestamp,
      |    some_boolean,
      |    optional_boolean,
      |    some_binary,
      |    optional_binary
      |) VALUES (
      |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      |);
      |""".stripMargin

  val CallInsert = "call insert_sp (?::FLOAT, ?::FLOAT, ?::DECIMAL, ?::DECIMAL, ?::FLOAT, ?::FLOAT)"
  val CallInsertAndReturnRows = "call insert_and_return_rows_sp (?::FLOAT, ?::FLOAT, ?::DECIMAL, ?::DECIMAL, ?::FLOAT, ?::FLOAT)"

  val AllFieldsRecord = SomeRecord(
    id = 111L,
    someInt = 123,
    optionalInt = Some(456),
    someBigInt = 789L,
    optionalBigInt = Some(101112L),
    someSmallInt = 1,
    optionalSmallInt = Some(2),
    someFloat = 1.23f,
    optionalFloat = Some(4.56f),
    someDouble = 7.89,
    optionalDouble = Some(10.11),
    someDecimal = BigDecimal("123.45"),
    optionalDecimal = Some(BigDecimal("678.90")),
    someChar = "char                ",
    optionalChar = Some("optionalChar        "),
    someVarchar = "varchar",
    optionalVarchar = Some("optionalVarchar"),
    someDate = java.sql.Date.valueOf("2024-01-01"),
    optionalDate = Some(java.sql.Date.valueOf("2024-02-02")),
    someTime = java.sql.Time.valueOf("12:34:56"),
    optionalTime = Some(java.sql.Time.valueOf("01:23:45")),
    someTimestamp = java.sql.Timestamp.valueOf("2024-01-01 12:34:56"),
    optionalTimestamp = Some(java.sql.Timestamp.valueOf("2024-02-02 01:23:45")),
    someBoolean = true,
    optionalBoolean = Some(false),
    someBinary = Array[Byte](1, 2, 3),
    optionalBinary = Some(Array[Byte](4, 5, 6))
  )

  val RequiredFieldsRecord = SomeRecord(
    id = 222L,
    someInt = 123,
    optionalInt = None,
    someBigInt = 789L,
    optionalBigInt = None,
    someSmallInt = 1,
    optionalSmallInt = None,
    someFloat = 1.23f,
    optionalFloat = None,
    someDouble = 7.89,
    optionalDouble = None,
    someDecimal = BigDecimal("123.45"),
    optionalDecimal = None,
    someChar = "char                ",
    optionalChar = None,
    someVarchar = "varchar",
    optionalVarchar = None,
    someDate = java.sql.Date.valueOf("2024-01-01"),
    optionalDate = None,
    someTime = java.sql.Time.valueOf("12:34:56"),
    optionalTime = None,
    someTimestamp = java.sql.Timestamp.valueOf("2024-01-01 12:34:56"),
    optionalTimestamp = None,
    someBoolean = true,
    optionalBoolean = None,
    someBinary = Array[Byte](1, 2, 3),
    optionalBinary = None
  )


  @BeforeEach
  def beforeEach(): Unit = {
    withConn { conn =>
      testJdbc.update(conn, "DELETE FROM some_record")
      testJdbc.update(conn, "DELETE FROM for_sp")
      ()
    }
  }

  @Test
  def execute(): Unit = {
    val tableName = s"random_table_${System.currentTimeMillis()}"
    val ddl =
      s"""
         |CREATE TABLE $tableName (
         |  random_column VARCHAR(10)
         |)""".stripMargin

    withConn { implicit conn =>
      jdbcRoutine.execute(ddl)
      testJdbc.execute(conn, s"Truncate Table $tableName").asInstanceOf[Unit]
    }
  }

  @Test
  def insertAllFieldsRecord(): Unit = {
    insertTest(AllFieldsRecord)
  }

  @Test
  def insertRequiredFieldsRecord(): Unit = {
    insertTest(RequiredFieldsRecord)
  }

  @Test
  def queryForSeq(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](AllFieldsRecord): _*)
      jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](RequiredFieldsRecord): _*)

      val rows = jdbcRoutine.queryForSeq("SELECT * FROM some_record where id in (?, ?, ?) ORDER BY id", new SomeRecordRowHandler(), -1L, RequiredFieldsRecord.id, AllFieldsRecord.id)
      assertEquals(2, rows.size)
      assertSameRecord(AllFieldsRecord, rows(0))
      assertSameRecord(RequiredFieldsRecord, rows(1))
    }
  }

  @Test
  def queryForSeq_wantValuePresentButNull(): Unit = {
    val tableName = s"random_table_${System.currentTimeMillis()}"
    val ddl =
      s"""
         |CREATE TABLE $tableName (
         |  id BIGINT PRIMARY KEY,
         |  optional_char CHAR(20)
         |)""".stripMargin

    withConn { implicit conn =>
      testJdbc.execute(conn, ddl)
      testJdbc.update(conn, s"insert into $tableName (id) values(?)", 1)
      val exception = assertThrows(classOf[SQLNonTransientException], () => jdbcRoutine.queryForSeq(s"select * from $tableName where id = ?", _.getString("OPTIONAL_CHAR"), 1).asInstanceOf[Unit])
      assertTrue(exception.getMessage.contains("OPTIONAL_CHAR"))

    }
  }

  @Test
  def queryForSingle(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](AllFieldsRecord): _*)

      assertEquals(Some(AllFieldsRecord.someChar), jdbcRoutine.queryForSingle("SELECT * FROM some_record where id = ?", _.getString("SOME_CHAR"), AllFieldsRecord.id))
      assertEquals(AllFieldsRecord.optionalChar, jdbcRoutine.queryForSingle("SELECT * FROM some_record where id = ?", _.getStringOpt("OPTIONAL_CHAR"), AllFieldsRecord.id).flatten)

      assertEquals(None, jdbcRoutine.queryForSingle("SELECT * FROM some_record where id = ?", _.getStringOpt("SOME_CHAR"), -1))
      assertEquals(None, jdbcRoutine.queryForSingle("SELECT * FROM some_record where id = ?", _.getStringOpt("OPTIONAL_CHAR"), -1))
    }
  }

  @Test
  def insertAndGetGeneratedKeys(): Unit = {
    val tableName = s"random_table_${System.currentTimeMillis()}"
    val ddl =
      s"""
         |CREATE TABLE $tableName (
         |  id BIGSERIAL PRIMARY KEY,
         |  some_int INT NOT NULL
         |  )
      """.stripMargin

    withConn { implicit conn =>
      testJdbc.execute(conn, ddl)

      val (affectedRows, keys) = jdbcRoutine.updateAndGetGeneratedKeys[Long](s"insert into $tableName(some_int) values(?)", (resultSet: WrappedResultSet) =>
        if (resultSet.next()) {
          Some(resultSet.getLong(1))
        } else {
          None
        }, 123)

      assertEquals(1, affectedRows)
      assertEquals(Some(1L), keys)
    }
  }

  @Test
  def updateRows(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](AllFieldsRecord): _*)
      jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](RequiredFieldsRecord): _*)

      assertEquals(1, jdbcRoutine.update("update some_record set some_int = ? where id = ? ", 999, AllFieldsRecord.id))

      val rows = jdbcRoutine.queryForSeq("SELECT * FROM some_record ORDER BY id", new SomeRecordRowHandler())
      assertSameRecord(AllFieldsRecord.copy(someInt = 999), rows(0))
      assertSameRecord(RequiredFieldsRecord, rows(1))
    }
  }

  @Test
  def updateAndGetGeneratedKeys_noGeneration(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](AllFieldsRecord): _*)

      val (affectedRows, keys) = jdbcRoutine.updateAndGetGeneratedKeys[Long](s"update some_record set some_int = 999 where id = ? ", (resultSet: WrappedResultSet) =>
        if (resultSet.next()) {
          Some(resultSet.getLong("ID"))
        } else {
          None
        }, -1)

      assertEquals(0, affectedRows)
      assertEquals(None, keys)
    }
  }


  @Test
  def delete(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](AllFieldsRecord): _*)
      jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](RequiredFieldsRecord): _*)

      jdbcRoutine.update("DELETE FROM some_record where id = ?", AllFieldsRecord.id)
      val rows = jdbcRoutine.queryForSeq("SELECT * FROM some_record ORDER BY id", new SomeRecordRowHandler())
      assertEquals(1, rows.size)
      assertSameRecord(RequiredFieldsRecord, rows(0))
    }
  }

  @Test
  def batchInsert(): Unit = {
    withConn { implicit conn =>
      assertArrayEquals(Array(1, 1), jdbcRoutine.batchUpdate(InsertSomeRecordSql, extractFieldValues[SomeRecord](AllFieldsRecord), extractFieldValues[SomeRecord](RequiredFieldsRecord)))

      val rows = jdbcRoutine.queryForSeq("SELECT * FROM some_record where id in (?, ?, ?) ORDER BY id", new SomeRecordRowHandler(), -1L, RequiredFieldsRecord.id, AllFieldsRecord.id)
      assertEquals(2, rows.size)
      assertSameRecord(AllFieldsRecord, rows(0))
      assertSameRecord(RequiredFieldsRecord, rows(1))
    }
  }

  @Test
  def callToUpdate_fillAllParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callToUpdate(CallInsert,
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
  def callToUpdate_fillRequiredParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callToUpdate(CallInsert,
        InParam(1.0F),
        InParam(None),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, None),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(-1, result.updateCount)
      assertEquals(Map(3 -> BigDecimal(200), 5 -> 2.0F), result.outValues)
    }
  }

  @Test
  def callForSeq_fillAllParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callForSeq(CallInsertAndReturnRows, new ForSpRowHandler,
        InParam(1.0F),
        InParam(Some(2.0F)),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, Some(200.0F)),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(1, result.records.size)
      assertEquals(ForSp(
        someFloat = 1.0F,
        optionalFloat = Some(2.0F),
        someDecimal = BigDecimal(100.0F),
        optionalDecimal = Some(BigDecimal(200.0F))), result.records(0))
      assertEquals(Map(3 -> BigDecimal(200), 4 -> BigDecimal(400), 5 -> 2.0F, 6 -> 4.0F), result.outValues)
    }
  }


  @Test
  def callForSeq_fillRequiredParams(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callForSeq(CallInsertAndReturnRows, new ForSpRowHandler,
        InParam(1.0F),
        InParam(None),
        InOutParam(Types.DECIMAL, 100.0F),
        InOutParam(Types.DECIMAL, None),
        OutParam(Types.FLOAT),
        OutParam(Types.FLOAT)
      )

      assertEquals(1, result.records.size)
      assertEquals(ForSp(
        someFloat = 1.0F,
        optionalFloat = None,
        someDecimal = BigDecimal(100.0F),
        optionalDecimal = None), result.records(0))
      assertEquals(Map(3 -> BigDecimal(200), 5 -> 2.0F), result.outValues)
    }
  }


  @Test
  def callToUpdate_againstSpWithResults(): Unit = {
    withConn { implicit conn =>
      val result = jdbcRoutine.callToUpdate(CallInsertAndReturnRows,
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
      val result = jdbcRoutine.callForSeq(CallInsert, new ForSpRowHandler,
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

  @Test
  def insertRowsInTransaction_allGood(): Unit = {
    withConn { implicit conn =>
      transaction {
        jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](AllFieldsRecord): _*)
        jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](RequiredFieldsRecord): _*)
      }
    }

    withConn { implicit conn =>
      val idList = jdbcRoutine.queryForSeq("SELECT id FROM some_record ORDER BY id", _.getLong("id"))
      assertEquals(Seq(AllFieldsRecord.id, RequiredFieldsRecord.id), idList)
    }
  }

  @Test
  def insertRowsInTransaction_withRollback(): Unit = {
    withConn { implicit conn =>
      try {
        transaction {
          jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](AllFieldsRecord): _*)
          jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](RequiredFieldsRecord.copy(someChar = "12345678901234567890_OVER_LONG")): _*)
        }
        fail("Shouldn't be here")
      } catch {
        case e: SQLException => println(e.getMessage)
      }
    }

    withConn { implicit conn =>
      val idList = jdbcRoutine.queryForSeq("SELECT id FROM some_record ORDER BY id", _.getLong("id"))
      assertEquals(Seq(), idList)
    }
  }


  private class SomeRecordRowHandler extends RowHandler[SomeRecord] {

    override def handle(rs: WrappedResultSet): SomeRecord = SomeRecord(
      id = rs.getLong("ID"),
      someInt = rs.getInt("SOME_INT"),
      optionalInt = rs.getIntOpt("OPTIONAL_INT"),
      someBigInt = rs.getLong("SOME_BIGINT"),
      optionalBigInt = rs.getLongOpt("OPTIONAL_BIGINT"),
      someSmallInt = rs.getShort("SOME_SMALLINT"),
      optionalSmallInt = rs.getShortOpt("OPTIONAL_SMALLINT"),
      someFloat = rs.getFloat("SOME_FLOAT"),
      optionalFloat = rs.getFloatOpt("OPTIONAL_FLOAT"),
      someDouble = rs.getDouble("SOME_DOUBLE"),
      optionalDouble = rs.getDoubleOpt("OPTIONAL_DOUBLE"),
      someDecimal = rs.getBigDecimal("SOME_DECIMAL"),
      optionalDecimal = rs.getBigDecimalOpt("OPTIONAL_DECIMAL"),
      someChar = rs.getString("SOME_CHAR"),
      optionalChar = rs.getStringOpt("OPTIONAL_CHAR"),
      someVarchar = rs.getString("SOME_VARCHAR"),
      optionalVarchar = rs.getStringOpt("OPTIONAL_VARCHAR"),
      someDate = rs.getDate("SOME_DATE"),
      optionalDate = rs.getDateOpt("OPTIONAL_DATE"),
      someTime = rs.getTime("SOME_TIME"),
      optionalTime = rs.getTimeOpt("OPTIONAL_TIME"),
      someTimestamp = rs.getTimestamp("SOME_TIMESTAMP"),
      optionalTimestamp = rs.getTimestampOpt("OPTIONAL_TIMESTAMP"),
      someBoolean = rs.getBoolean("SOME_BOOLEAN"),
      optionalBoolean = rs.getBooleanOpt("OPTIONAL_BOOLEAN"),
      someBinary = rs.getBytes("SOME_BINARY"),
      optionalBinary = rs.getBytesOpt("OPTIONAL_BINARY")
    )
  }

  private class ForSpRowHandler extends RowHandler[ForSp] {
    override def handle(rs: WrappedResultSet): ForSp = ForSp(
      someFloat = rs.getFloat("SOME_FLOAT"),
      optionalFloat = rs.getFloatOpt("OPTIONAL_FLOAT"),
      someDecimal = rs.getBigDecimal("SOME_DECIMAL"),
      optionalDecimal = rs.getBigDecimalOpt("OPTIONAL_DECIMAL")
    )
  }


  //TODO: use parameterized test
  private def insertTest(record: SomeRecord): Unit = {
    withConn { conn =>
      assertEquals(1, jdbcRoutine.update(InsertSomeRecordSql, extractFieldValues[SomeRecord](record): _*)(conn))
      val row = testJdbc.query(conn, s"select * from some_record", new MapListHandler()).get(0)
      assertSameData(record, row)
    }
  }

  private def assertSameRecord(expected: SomeRecord, actual: SomeRecord): Unit = {
    val theByteArray = Array[Byte]()
    assertEquals(expected.copy(someBinary = theByteArray, optionalBinary = None), actual.copy(someBinary = theByteArray, optionalBinary = None))
    assertArrayEquals(expected.someBinary, actual.someBinary)
    (expected.optionalBinary, actual.optionalBinary) match {
      case (None, None) =>
      case (Some(exp), Some(act)) => assertArrayEquals(exp, act)
      case (_, _) => fail()
    }
  }

  private def assertSameData(record: SomeRecord, row: util.Map[String, Object]): Unit = {
    assertEquals(record.id, row.get("ID"))
    assertEquals(record.someInt, row.get("SOME_INT"))
    assertEquals(record.optionalInt, Option(row.get("OPTIONAL_INT")))
    assertEquals(record.someBigInt, row.get("SOME_BIGINT"))
    assertEquals(record.optionalBigInt, Option(row.get("OPTIONAL_BIGINT")))
    assertEquals(record.someSmallInt.toInt, row.get("SOME_SMALLINT"))
    assertEquals(record.optionalSmallInt.map(_.toInt), Option(row.get("OPTIONAL_SMALLINT")))
    assertEquals(record.someFloat.toDouble, row.get("SOME_FLOAT"))
    assertEquals(record.optionalFloat.map(_.toDouble), Option(row.get("OPTIONAL_FLOAT")))
    assertEquals(record.someDouble, row.get("SOME_DOUBLE"))
    assertEquals(record.optionalDouble, Option(row.get("OPTIONAL_DOUBLE")))
    assertEquals(record.someDecimal.bigDecimal, row.get("SOME_DECIMAL"))
    assertEquals(record.optionalDecimal.map(_.bigDecimal), Option(row.get("OPTIONAL_DECIMAL")))
    assertEquals(record.someChar, row.get("SOME_CHAR"))
    assertEquals(record.optionalChar, Option(row.get("OPTIONAL_CHAR")))
    assertEquals(record.someVarchar, row.get("SOME_VARCHAR"))
    assertEquals(record.optionalVarchar, Option(row.get("OPTIONAL_VARCHAR")))
    assertEquals(record.someDate, row.get("SOME_DATE"))
    assertEquals(record.optionalDate, Option(row.get("OPTIONAL_DATE")))
    assertEquals(record.someTime, row.get("SOME_TIME"))
    assertEquals(record.optionalTime, Option(row.get("OPTIONAL_TIME")))
    assertEquals(record.someTimestamp, row.get("SOME_TIMESTAMP"))
    assertEquals(record.optionalTimestamp, Option(row.get("OPTIONAL_TIMESTAMP")))
    assertEquals(record.someBoolean, row.get("SOME_BOOLEAN"))
    assertEquals(record.optionalBoolean, Option(row.get("OPTIONAL_BOOLEAN")))
    assertEquals(record.someBinary.toSeq, row.get("SOME_BINARY").asInstanceOf[Array[Byte]].toSeq)
    assertEquals(record.optionalBinary.map(_.toSeq), Option(row.get("OPTIONAL_BINARY")).map(_.asInstanceOf[Array[Byte]].toSeq))
  }
}
