package org.sjr.it.vendor.common.support

import java.sql.{Connection, DriverManager}
import scala.util.Using

package object special {
  /**
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
