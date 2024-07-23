package org.sjr.it.vendor.mysql

import org.testcontainers.containers.MySQLContainer

package object support {

  def createMySQLContainer(): MySQLContainer[_] = {
    new MySQLContainer("mysql")
  }
}
