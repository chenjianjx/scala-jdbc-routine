package org.sjr.it.vendor.sqlserver

import org.testcontainers.containers.MSSQLServerContainer

package object support {

  def createSqlServerContainer(): MSSQLServerContainer[_] = {
    new MSSQLServerContainer("mcr.microsoft.com/mssql/server").acceptLicense()
  }
}
