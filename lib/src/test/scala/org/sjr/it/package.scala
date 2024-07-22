package org.sjr

import org.apache.commons.dbutils.QueryRunner
import org.testcontainers.containers.MySQLContainer

import java.sql
import java.sql.{Connection, DriverManager}
import scala.util.Using

package object it {

  def createTestContainer(): MySQLContainer[Nothing] = {
    new MySQLContainer("mysql")
  }

  def testJdbc = {
    new QueryRunner()
  }

  def withConn[T](job: Connection => T)(implicit testContainer: MySQLContainer[Nothing]): T = {
    Using.resource(DriverManager.getConnection(testContainer.getJdbcUrl, testContainer.getUsername, testContainer.getPassword)) { conn =>
      job(conn)
    }
  }


  def sqlArrayToSeq(sqlArray: sql.Array): Seq[AnyRef] = {
    sqlArray.getArray.asInstanceOf[Array[AnyRef]].toSeq
  }

  /**
   *
   * @return the jdbc url
   */
  def createH2Db(clazz: Class[_]): String = {
    s"jdbc:h2:./build/db/${clazz.getSimpleName}-${System.currentTimeMillis()};DB_CLOSE_DELAY=-1;MODE=MySQL"
  }


  def withH2Conn[T](job: Connection => T)(implicit h2JdbcUrl: String): T = {
    Using.resource(DriverManager.getConnection(h2JdbcUrl, "sa", "")) { conn =>
      job(conn)
    }
  }
}
