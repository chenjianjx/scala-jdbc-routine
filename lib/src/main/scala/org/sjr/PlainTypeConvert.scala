package org.sjr

object PlainTypeConvert {

  def scalaValueToJavaValueForJdbc(scalaValue: Any): Any = {
    scalaValue match {
      case Some(v: BigDecimal) => v.bigDecimal
      case Some(v) => v
      case None => None.orNull
      case v: BigDecimal => v.bigDecimal
      case _ => scalaValue
    }
  }

  def nonNullJavaValueFromJdbcToScalaValue(javaValue: Any): Any = {
    javaValue match {
      case v: java.math.BigDecimal => BigDecimal(v)
      case _ => javaValue
    }
  }
}
