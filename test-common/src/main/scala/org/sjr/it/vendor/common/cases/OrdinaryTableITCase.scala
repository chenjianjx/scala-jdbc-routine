package org.sjr.it.vendor.common.cases

import org.apache.commons.dbutils.handlers.MapListHandler
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.sjr.RowHandler
import org.sjr.TransactionRoutine.transaction
import org.sjr.it.vendor.common.support.{testJdbc, withConn}

import java.sql.{Connection, SQLException, SQLNonTransientException}
import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala



trait AllTypesRecord[T <: AllTypesRecord[T]] {
  def id: Long
  def string: String
  def stringOpt: Option[String]
  def makeCopy(string: String): T
}


abstract class OrdinaryTableITCase[T <: AllTypesRecord[T]] extends VendorITCaseBase {

  protected def tableName: String

  protected def ddlsForTable: Seq[String]

  protected def insertSql: String

  protected def idColumnName: String

  protected def stringColumnName: String

  protected def stringOptColumnName: String

  protected def recordToParams(record: T)(implicit conn: Connection): Seq[Any]

  protected def getRowHandler: RowHandler[T]

  protected def getByIndexRowHandler: RowHandler[T]

  protected def recordWithTotalFields: T

  protected def recordWithRequiredFields: T

  protected def assertDataInDb(expected: T, recordInDb: util.Map[String, Object]): Unit


  override protected def perClassInit(): Unit = {
    withConn { conn =>
      ddlsForTable.foreach(testJdbc.execute(conn, _))
      ()
    }
  }

  @BeforeEach
  def beforeEach(): Unit = {
    withConn { conn =>
      testJdbc.update(conn, s"DELETE FROM $tableName")
      ()
    }
  }

  @Test
  def insert(): Unit = {
    withConn { implicit conn =>
      assertEquals(1, jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*))
      assertEquals(1, jdbcRoutine.update(insertSql, recordToParams(recordWithRequiredFields): _*))
      val recordsInDb = testJdbc.query(conn, s"select * from $tableName order by $idColumnName", new MapListHandler()).asScala.toSeq
      assertEquals(2, recordsInDb.size)
      assertDataInDb(recordWithTotalFields, recordsInDb(0))
      assertDataInDb(recordWithRequiredFields, recordsInDb(1))
    }
  }

  @Test
  def queryForSeq(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*)
      jdbcRoutine.update(insertSql, recordToParams(recordWithRequiredFields): _*)

      val rows = jdbcRoutine.queryForSeq(s"SELECT * FROM $tableName where $idColumnName in (?, ?, ?) ORDER BY $idColumnName", getRowHandler, -1L, recordWithRequiredFields.id, recordWithTotalFields.id)
      assertEquals(2, rows.size)
      assertEquals(recordWithTotalFields, rows(0))
      assertEquals(recordWithRequiredFields, rows(1))
    }
  }


  @Test
  def queryForSeq_byIndexRowHandler(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*)
      jdbcRoutine.update(insertSql, recordToParams(recordWithRequiredFields): _*)

      val rows = jdbcRoutine.queryForSeq(s"SELECT * FROM $tableName where $idColumnName in (?, ?, ?) ORDER BY $idColumnName", getByIndexRowHandler, -1L, recordWithRequiredFields.id, recordWithTotalFields.id)
      assertEquals(2, rows.size)
      assertEquals(recordWithTotalFields, rows(0))
      assertEquals(recordWithRequiredFields, rows(1))
    }
  }


  @Test
  def queryForSingleRow(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*)
      val rows = jdbcRoutine.queryForSeq(s"SELECT * FROM $tableName where $idColumnName = ?", getRowHandler, recordWithTotalFields.id)
      assertEquals(1, rows.size)
      assertEquals(recordWithTotalFields, rows(0))
    }
  }

  @Test
  def queryForSingleRowSingleColumn(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*)

      assertEquals(Some(recordWithTotalFields.string), jdbcRoutine.queryForSingle(s"SELECT * FROM $tableName where $idColumnName = ?", _.getString(stringColumnName.toUpperCase), recordWithTotalFields.id))
      assertEquals(recordWithTotalFields.stringOpt, jdbcRoutine.queryForSingle(s"SELECT * FROM $tableName where $idColumnName = ?", _.getStringOpt(stringOptColumnName.toUpperCase), recordWithTotalFields.id).flatten)

      assertEquals(None, jdbcRoutine.queryForSingle(s"SELECT * FROM $tableName where $idColumnName = ?", _.getStringOpt(stringColumnName.toUpperCase), -1))
      assertEquals(None, jdbcRoutine.queryForSingle(s"SELECT * FROM $tableName where $idColumnName = ?", _.getStringOpt(stringOptColumnName.toUpperCase), -1))
    }
  }


  @Test
  def query_wantValuePresentButNull(): Unit = {

    withConn { implicit conn =>
      jdbcRoutine.update(insertSql, recordToParams(recordWithRequiredFields): _*)

      val exception = assertThrows(classOf[SQLNonTransientException], () => jdbcRoutine.queryForSeq(s"select * from $tableName where $idColumnName = ?", _.getString(stringOptColumnName.toUpperCase), recordWithRequiredFields.id).asInstanceOf[Unit])
      assertTrue(exception.getMessage.contains(stringOptColumnName.toUpperCase))

    }
  }

  @Test
  def updateRows(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*)
      jdbcRoutine.update(insertSql, recordToParams(recordWithRequiredFields): _*)

      assertEquals(1, jdbcRoutine.update(s"UPDATE $tableName SET $stringColumnName = ? where $idColumnName = ? ", "999", recordWithTotalFields.id))

      val rows = jdbcRoutine.queryForSeq(s"SELECT * FROM $tableName ORDER BY $idColumnName", getRowHandler)
      assertEquals(recordWithTotalFields.makeCopy(string = "999"), rows(0))
      assertEquals(recordWithRequiredFields, rows(1))
    }
  }


  @Test
  def delete(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*)
      jdbcRoutine.update(insertSql, recordToParams(recordWithRequiredFields): _*)

      jdbcRoutine.update(s"DELETE FROM $tableName where $idColumnName = ?", recordWithTotalFields.id)
      val rows = jdbcRoutine.queryForSeq(s"SELECT * FROM $tableName ORDER BY $idColumnName", getRowHandler)
      assertEquals(1, rows.size)
      assertEquals(recordWithRequiredFields, rows(0))
    }
  }

  @Test
  def batchInsert(): Unit = {
    withConn { implicit conn =>
      assertArrayEquals(scala.Array(1, 1), jdbcRoutine.batchUpdate(insertSql, recordToParams(recordWithTotalFields), recordToParams(recordWithRequiredFields)))

      val rows = jdbcRoutine.queryForSeq(s"SELECT * FROM $tableName where $idColumnName in (?, ?, ?) ORDER BY $idColumnName", getRowHandler, -1L, recordWithRequiredFields.id, recordWithTotalFields.id)
      assertEquals(2, rows.size)
      assertEquals(recordWithTotalFields, rows(0))
      assertEquals(recordWithRequiredFields, rows(1))
    }
  }


  @Test
  def insertRowsInTransaction_allGood(): Unit = {
    withConn { implicit conn =>
      transaction {
        jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*)
        jdbcRoutine.update(insertSql, recordToParams(recordWithRequiredFields): _*)
      }
    }

    withConn { implicit conn =>
      val idSeq = jdbcRoutine.queryForSeq(s"SELECT $idColumnName FROM $tableName ORDER BY $idColumnName", _.getScalaLong("id"))
      assertEquals(Seq(recordWithTotalFields.id, recordWithRequiredFields.id), idSeq)
    }
  }

  @Test
  def insertRowsInTransaction_withRollback(): Unit = {
    withConn { implicit conn =>
      try {
        transaction {
          jdbcRoutine.update(insertSql, recordToParams(recordWithTotalFields): _*)
          jdbcRoutine.update(insertSql, recordToParams(recordWithRequiredFields.makeCopy(string = "12345678901234567890_OVER_LONG")): _*)
        }
        fail("Shouldn't be here")
      } catch {
        case e: SQLException => println(e.getMessage)
      }
    }

    withConn { implicit conn =>
      val idSeq = jdbcRoutine.queryForSeq(s"SELECT $idColumnName FROM $tableName ORDER BY $idColumnName", _.getScalaLong("id"))
      assertEquals(Seq(), idSeq)
    }
  }


}
