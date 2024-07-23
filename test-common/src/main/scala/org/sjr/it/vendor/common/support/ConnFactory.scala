package org.sjr.it.vendor.common.support

import org.testcontainers.containers.JdbcDatabaseContainer

import java.sql.{Connection, DriverManager}

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
