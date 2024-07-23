package org.sjr.it.vendor.common.cases

import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, TestInstance}
import org.sjr.JdbcRoutine
import org.sjr.it.vendor.common.support.ConnFactory

@TestInstance(Lifecycle.PER_CLASS)
abstract class VendorITCaseBase {
  protected implicit lazy val connFactory: ConnFactory = getConnFactory()

  protected def getConnFactory(): ConnFactory

  protected def perClassInit(): Unit

  protected val jdbcRoutine = new JdbcRoutine

  @BeforeAll
  def beforeAll(): Unit = {
    connFactory.start()
    perClassInit()
  }

  @AfterAll
  def afterAll(): Unit = {
    connFactory.stop()
  }
}
