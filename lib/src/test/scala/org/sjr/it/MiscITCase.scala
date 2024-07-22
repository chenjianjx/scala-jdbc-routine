package org.sjr.it

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.sjr.{GeneratedKeysHandler, JdbcRoutine, WrappedResultSet}
import org.testcontainers.containers.MySQLContainer

object MiscITCase {
  private implicit val testContainer: MySQLContainer[Nothing]  = createTestContainer()

  private val TableWithAutoKey =
    s"""
       |CREATE TABLE auto_key_record (
       |  id BIGINT AUTO_INCREMENT PRIMARY KEY,
       |  int_value INT NOT NULL
       |  )
      """.stripMargin

  @BeforeAll
  def beforeAll(): Unit = {
    testContainer.start()

    withConn { conn =>
      testJdbc.execute(conn, TableWithAutoKey)
    }
    ()
  }

  @AfterAll
  def afterAll(): Unit = {
    testContainer.close()
  }
}


class MiscITCase {

  import MiscITCase.testContainer

  private val jdbcRoutine = new JdbcRoutine

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
      testJdbc.execute(conn, s"Truncate Table $tableName") //should succeed
      ()
    }
  }


  @Test
  def insertAndGetGeneratedKeys(): Unit = {

    withConn { implicit conn =>

      val (affectedRows, keys) = jdbcRoutine.updateAndGetGeneratedKeys[Long](s"insert into auto_key_record(int_value) values(?)", new AutoKeyRecordGeneratedKeysHandler, 123)

      assertEquals(1, affectedRows)
      assertEquals(Some(1L), keys)
    }
  }


  @Test
  def updateAndGetGeneratedKeys_noGeneration(): Unit = {
    withConn { implicit conn =>

      val (affectedRows, keys) = jdbcRoutine.updateAndGetGeneratedKeys[Long](s"UPDATE auto_key_record SET int_value = 999 where id = ? ", new AutoKeyRecordGeneratedKeysHandler, -1)

      assertEquals(0, affectedRows)
      assertEquals(None, keys)
    }
  }


  @Test
  def getObjectFromResultSet(): Unit = {
    withConn { implicit conn =>

      val sql = "SELECT 0 as value"

      assertEquals(Some(0), jdbcRoutine.queryForSingle(sql, row => row.getObject(1)))
      assertEquals(Some(0), jdbcRoutine.queryForSingle(sql, row => row.getObject("value")))

      assertEquals(Some(0L), jdbcRoutine.queryForSingle(sql, row => row.getObject(1, classOf[Long])))
      assertEquals(Some(0L), jdbcRoutine.queryForSingle(sql, row => row.getObject("value", classOf[Long])))
    }
  }


  private class AutoKeyRecordGeneratedKeysHandler extends GeneratedKeysHandler[Long] {

    override def handle(resultSet: WrappedResultSet): Option[Long] = if (resultSet.next()) {
      Some(resultSet.getLong(1))
    } else {
      None
    }
  }
}
