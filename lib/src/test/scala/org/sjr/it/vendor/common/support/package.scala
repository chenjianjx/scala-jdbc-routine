package org.sjr.it.vendor.common

import org.apache.commons.dbutils.QueryRunner
import org.testcontainers.containers.JdbcDatabaseContainer

import java.sql.{Connection, DriverManager}
import scala.util.Using

package object support {

  trait ConnFactory {
    def start(): Unit

    def getConn(): Connection

    def stop(): Unit
  }

  class TestContainerConnFactory(container: JdbcDatabaseContainer[_]) extends ConnFactory {
    override def getConn(): Connection = DriverManager.getConnection(container.getJdbcUrl, container.getUsername, container.getPassword)

    override def stop(): Unit = container.close()

    override def start(): Unit = container.start()
  }


  def withConn[T](job: Connection => T)(implicit connFactory: ConnFactory): T = {
    Using.resource(connFactory.getConn()) { conn =>
      job(conn)
    }
  }

  def testJdbc: QueryRunner = {
    new QueryRunner()
  }



}
