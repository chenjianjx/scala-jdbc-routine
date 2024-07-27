package org.sjr

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql
import java.sql.{Blob, Clob, Date, NClob, ResultSet, SQLNonTransientException, SQLXML, Time, Timestamp}


/**
 * The retrieval methods in vanilla ResultSet can return nulls, which is frowned upon in Scala.
 * This class lets you get value as Option, or throw an SQLException when the value is null but you expect it to be present
 *
 * Also note retrieval methods for primitive types such as Int, Boolean won't return 0 or false if the value is SQL NULL
 *
 * @param delegate
 */
class WrappedResultSet(private val delegate: ResultSet) {


  def getArray(columnIndex: Int): sql.Array = getOrThrow(columnIndex, getArrayOpt(columnIndex))

  def getArray(columnLabel: String): sql.Array = getOrThrow(columnLabel, getArrayOpt(columnLabel))

  def getArrayOpt(columnIndex: Int): Option[sql.Array] = Option(delegate.getArray(columnIndex))

  def getArrayOpt(columnLabel: String): Option[sql.Array] = Option(delegate.getArray(columnLabel))

  def getAsciiStream(columnIndex: Int): InputStream = getOrThrow(columnIndex, getAsciiStreamOpt(columnIndex))

  def getAsciiStream(columnLabel: String): InputStream = getOrThrow(columnLabel, getAsciiStreamOpt(columnLabel))

  def getAsciiStreamOpt(columnIndex: Int): Option[InputStream] = Option(delegate.getAsciiStream(columnIndex))

  def getAsciiStreamOpt(columnLabel: String): Option[InputStream] = Option(delegate.getAsciiStream(columnLabel))

  def getBigDecimal(columnIndex: Int): scala.math.BigDecimal = getOrThrow(columnIndex, getBigDecimalOpt(columnIndex))

  def getBigDecimal(columnLabel: String): scala.math.BigDecimal = getOrThrow(columnLabel, getBigDecimalOpt(columnLabel))

  def getBigDecimalOpt(columnIndex: Int): Option[scala.math.BigDecimal] = Option(delegate.getBigDecimal(columnIndex)).map(BigDecimal(_))

  def getBigDecimalOpt(columnLabel: String): Option[scala.math.BigDecimal] = Option(delegate.getBigDecimal(columnLabel)).map(BigDecimal(_))

  def getBinaryStream(columnIndex: Int): InputStream = getOrThrow(columnIndex, getBinaryStreamOpt(columnIndex))

  def getBinaryStream(columnLabel: String): InputStream = getOrThrow(columnLabel, getBinaryStreamOpt(columnLabel))

  def getBinaryStreamOpt(columnIndex: Int): Option[InputStream] = Option(delegate.getBinaryStream(columnIndex))

  def getBinaryStreamOpt(columnLabel: String): Option[InputStream] = Option(delegate.getBinaryStream(columnLabel))

  def getBlob(columnIndex: Int): Blob = getOrThrow(columnIndex, getBlobOpt(columnIndex))

  def getBlob(columnLabel: String): Blob = getOrThrow(columnLabel, getBlobOpt(columnLabel))

  def getBlobOpt(columnIndex: Int): Option[Blob] = Option(delegate.getBlob(columnIndex))

  def getBlobOpt(columnLabel: String): Option[Blob] = Option(delegate.getBlob(columnLabel))

  /**
   * Won't default to false if value doesn't exist. Instead an SQLException will be thrown.
   */
  def getScalaBoolean(columnIndex: Int): Boolean = getOrThrow(columnIndex, getScalaBooleanOpt(columnIndex))

  def getScalaBoolean(columnLabel: String): Boolean = getOrThrow(columnLabel, getScalaBooleanOpt(columnLabel))

  def getScalaBooleanOpt(columnIndex: Int): Option[Boolean] = primitiveOption(delegate.getBoolean(columnIndex))

  def getScalaBooleanOpt(columnLabel: String): Option[Boolean] = primitiveOption(delegate.getBoolean(columnLabel))

  /**
   * Won't default to 0 if value doesn't exist. Instead an SQLException will be thrown.
   */
  def getScalaByte(columnIndex: Int): Byte = getOrThrow(columnIndex, getScalaByteOpt(columnIndex))

  def getScalaByte(columnLabel: String): Byte = getOrThrow(columnLabel, getScalaByteOpt(columnLabel))

  def getScalaByteOpt(columnIndex: Int): Option[Byte] = primitiveOption(delegate.getByte(columnIndex))

  def getScalaByteOpt(columnLabel: String): Option[Byte] = primitiveOption(delegate.getByte(columnLabel))

  def getBytes(columnIndex: Int): scala.Array[Byte] = getOrThrow(columnIndex, getBytesOpt(columnIndex))

  def getBytes(columnLabel: String): scala.Array[Byte] = getOrThrow(columnLabel, getBytesOpt(columnLabel))

  def getBytesOpt(columnIndex: Int): Option[scala.Array[Byte]] = Option(delegate.getBytes(columnIndex))

  def getBytesOpt(columnLabel: String): Option[scala.Array[Byte]] = Option(delegate.getBytes(columnLabel))

  def getCharacterStream(columnIndex: Int): Reader = getOrThrow(columnIndex, getCharacterStreamOpt(columnIndex))

  def getCharacterStream(columnLabel: String): Reader = getOrThrow(columnLabel, getCharacterStreamOpt(columnLabel))

  def getCharacterStreamOpt(columnIndex: Int): Option[Reader] = Option(delegate.getCharacterStream(columnIndex))

  def getCharacterStreamOpt(columnLabel: String): Option[Reader] = Option(delegate.getCharacterStream(columnLabel))

  def getClob(columnIndex: Int): Clob = getOrThrow(columnIndex, getClobOpt(columnIndex))

  def getClob(columnLabel: String): Clob = getOrThrow(columnLabel, getClobOpt(columnLabel))

  def getClobOpt(columnIndex: Int): Option[Clob] = Option(delegate.getClob(columnIndex))

  def getClobOpt(columnLabel: String): Option[Clob] = Option(delegate.getClob(columnLabel))

  def getDate(columnIndex: Int): Date = getOrThrow(columnIndex, getDateOpt(columnIndex))

  def getDate(columnLabel: String): Date = getOrThrow(columnLabel, getDateOpt(columnLabel))

  def getDateOpt(columnIndex: Int): Option[Date] = Option(delegate.getDate(columnIndex))

  def getDateOpt(columnLabel: String): Option[Date] = Option(delegate.getDate(columnLabel))

  /**
   * Won't default to 0 if value doesn't exist. Instead an SQLException will be thrown.
   */
  def getScalaDouble(columnIndex: Int): Double = getOrThrow(columnIndex, getScalaDoubleOpt(columnIndex))

  def getScalaDouble(columnLabel: String): Double = getOrThrow(columnLabel, getScalaDoubleOpt(columnLabel))

  def getScalaDoubleOpt(columnIndex: Int): Option[Double] = primitiveOption(delegate.getDouble(columnIndex))

  def getScalaDoubleOpt(columnLabel: String): Option[Double] = primitiveOption(delegate.getDouble(columnLabel))

  /**
   * Won't default to 0 if value doesn't exist. Instead an SQLException will be thrown.
   */
  def getScalaFloat(columnIndex: Int): Float = getOrThrow(columnIndex, getScalaFloatOpt(columnIndex))

  def getScalaFloat(columnLabel: String): Float = getOrThrow(columnLabel, getScalaFloatOpt(columnLabel))

  def getScalaFloatOpt(columnIndex: Int): Option[Float] = primitiveOption(delegate.getFloat(columnIndex))

  def getScalaFloatOpt(columnLabel: String): Option[Float] = primitiveOption(delegate.getFloat(columnLabel))

  /**
   * Won't default to 0 if value doesn't exist. Instead an SQLException will be thrown.
   */
  def getScalaInt(columnIndex: Int): Int = getOrThrow(columnIndex, getScalaIntOpt(columnIndex))

  def getScalaInt(columnLabel: String): Int = getOrThrow(columnLabel, getScalaIntOpt(columnLabel))

  def getScalaIntOpt(columnIndex: Int): Option[Int] = primitiveOption(delegate.getInt(columnIndex))

  def getScalaIntOpt(columnLabel: String): Option[Int] = primitiveOption(delegate.getInt(columnLabel))

  /**
   * Won't default to 0 if value doesn't exist. Instead an SQLException will be thrown.
   */
  def getScalaLong(columnIndex: Int): Long = getOrThrow(columnIndex, getScalaLongOpt(columnIndex))

  def getScalaLong(columnLabel: String): Long = getOrThrow(columnLabel, getScalaLongOpt(columnLabel))

  def getScalaLongOpt(columnIndex: Int): Option[Long] = primitiveOption(delegate.getLong(columnIndex))

  def getScalaLongOpt(columnLabel: String): Option[Long] = primitiveOption(delegate.getLong(columnLabel))

  def getNCharacterStream(columnIndex: Int): Reader = getOrThrow(columnIndex, getNCharacterStreamOpt(columnIndex))

  def getNCharacterStream(columnLabel: String): Reader = getOrThrow(columnLabel, getNCharacterStreamOpt(columnLabel))

  def getNCharacterStreamOpt(columnIndex: Int): Option[Reader] = Option(delegate.getNCharacterStream(columnIndex))

  def getNCharacterStreamOpt(columnLabel: String): Option[Reader] = Option(delegate.getNCharacterStream(columnLabel))

  def getNClob(columnIndex: Int): NClob = getOrThrow(columnIndex, getNClobOpt(columnIndex))

  def getNClob(columnLabel: String): NClob = getOrThrow(columnLabel, getNClobOpt(columnLabel))

  def getNClobOpt(columnIndex: Int): Option[NClob] = Option(delegate.getNClob(columnIndex))

  def getNClobOpt(columnLabel: String): Option[NClob] = Option(delegate.getNClob(columnLabel))

  def getNString(columnIndex: Int): String = getOrThrow(columnIndex, getNStringOpt(columnIndex))

  def getNString(columnLabel: String): String = getOrThrow(columnLabel, getNStringOpt(columnLabel))

  def getNStringOpt(columnIndex: Int): Option[String] = Option(delegate.getNString(columnIndex))

  def getNStringOpt(columnLabel: String): Option[String] = Option(delegate.getNString(columnLabel))

  def getObject(columnIndex: Int): AnyRef = getOrThrow(columnIndex, getObjectOpt(columnIndex))

  def getObject(columnLabel: String): AnyRef = getOrThrow(columnLabel, getObjectOpt(columnLabel))

  def getObjectOpt(columnIndex: Int): Option[AnyRef] = Option(delegate.getObject(columnIndex))

  def getObjectOpt(columnLabel: String): Option[AnyRef] = Option(delegate.getObject(columnLabel))

  def getObjectOpt[T](columnIndex: Int, clazz: Class[T]): Option[T] = Option(delegate.getObject[T](columnIndex, clazz))

  def getObjectOpt[T](columnLabel: String, clazz: Class[T]): Option[T] = Option(delegate.getObject[T](columnLabel, clazz))

  def getObject[T](columnIndex: Int, clazz: Class[T]): T = getOrThrow(columnIndex, getObjectOpt[T](columnIndex, clazz))

  def getObject[T](columnLabel: String, clazz: Class[T]): T = getOrThrow(columnLabel, getObjectOpt[T](columnLabel, clazz))

  /**
   * Won't default to 0 if value doesn't exist. Instead an SQLException will be thrown.
   */
  def getScalaShort(columnIndex: Int): Short = getOrThrow(columnIndex, getScalaShortOpt(columnIndex))

  def getScalaShort(columnLabel: String): Short = getOrThrow(columnLabel, getScalaShortOpt(columnLabel))

  def getScalaShortOpt(columnIndex: Int): Option[Short] = primitiveOption(delegate.getShort(columnIndex))

  def getScalaShortOpt(columnLabel: String): Option[Short] = primitiveOption(delegate.getShort(columnLabel))

  def getSQLXML(columnIndex: Int): SQLXML = getOrThrow(columnIndex, getSQLXMLOpt(columnIndex))

  def getSQLXML(columnLabel: String): SQLXML = getOrThrow(columnLabel, getSQLXMLOpt(columnLabel))

  def getSQLXMLOpt(columnIndex: Int): Option[SQLXML] = Option(delegate.getSQLXML(columnIndex))

  def getSQLXMLOpt(columnLabel: String): Option[SQLXML] = Option(delegate.getSQLXML(columnLabel))

  def getString(columnIndex: Int): String = getOrThrow(columnIndex, getStringOpt(columnIndex))

  def getString(columnLabel: String): String = getOrThrow(columnLabel, getStringOpt(columnLabel))

  def getStringOpt(columnIndex: Int): Option[String] = Option(delegate.getString(columnIndex))

  def getStringOpt(columnLabel: String): Option[String] = Option(delegate.getString(columnLabel))

  def getTime(columnIndex: Int): Time = getOrThrow(columnIndex, getTimeOpt(columnIndex))

  def getTime(columnLabel: String): Time = getOrThrow(columnLabel, getTimeOpt(columnLabel))

  def getTimeOpt(columnIndex: Int): Option[Time] = Option(delegate.getTime(columnIndex))

  def getTimeOpt(columnLabel: String): Option[Time] = Option(delegate.getTime(columnLabel))

  def getTimestamp(columnIndex: Int): Timestamp = getOrThrow(columnIndex, getTimestampOpt(columnIndex))

  def getTimestamp(columnLabel: String): Timestamp = getOrThrow(columnLabel, getTimestampOpt(columnLabel))

  def getTimestampOpt(columnIndex: Int): Option[Timestamp] = Option(delegate.getTimestamp(columnIndex))

  def getTimestampOpt(columnLabel: String): Option[Timestamp] = Option(delegate.getTimestamp(columnLabel))

  def getURL(columnIndex: Int): URL = getOrThrow(columnIndex, getURLOpt(columnIndex))

  def getURL(columnLabel: String): URL = getOrThrow(columnLabel, getURLOpt(columnLabel))

  def getURLOpt(columnIndex: Int): Option[URL] = Option(delegate.getURL(columnIndex))

  def getURLOpt(columnLabel: String): Option[URL] = Option(delegate.getURL(columnLabel))

  def next(): Boolean = delegate.next()

  private def getOrThrow[T](columnRef: Any, value: Option[T]): T = {
    value.getOrElse(throw new SQLNonTransientException(s"Null value found when getting non-nullable value from column `$columnRef` . Try using `getXxxOpt()` method instead ?"))
  }

  private def primitiveOption[T](get: => T): Option[T] = {
    val value = get
    if (delegate.wasNull()) {
      None
    } else {
      Some(value)
    }
  }

}
