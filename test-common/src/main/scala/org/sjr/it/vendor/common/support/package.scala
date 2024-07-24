package org.sjr.it.vendor.common

import org.apache.commons.dbutils.QueryRunner

import java.sql.Connection
import scala.util.Using

package object support {


  def withConn[T](job: Connection => T)(implicit connFactory: ConnFactory): T = {
    Using.resource(connFactory.getConn()) { conn =>
      job(conn)
    }
  }

  def testJdbc: QueryRunner = {
    new QueryRunner()
  }



}
