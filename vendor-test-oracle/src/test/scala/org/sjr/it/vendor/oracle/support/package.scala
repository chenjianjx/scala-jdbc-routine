package org.sjr.it.vendor.oracle

import org.testcontainers.oracle.OracleContainer

package object support {

  def createOracleContainer(): OracleContainer = {
    new OracleContainer("gvenzl/oracle-free")
  }
}
