package org.sjr.it.vendor.postgresql

import org.testcontainers.containers.PostgreSQLContainer

package object support {

  def createPgContainer(): PostgreSQLContainer[_] = {
    new PostgreSQLContainer("postgres")
  }
}
