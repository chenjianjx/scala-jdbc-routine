package org.sjr.it.vendor.postgresql.cases

import org.junit.jupiter.api.Assertions.assertEquals
import org.sjr.it.vendor.common.cases.{AllTypesRecord, OrdinaryTableITCase}
import org.sjr.it.vendor.common.support
import org.sjr.it.vendor.common.support.TestContainerConnFactory
import org.sjr.it.vendor.postgresql.support.createPgContainer
import org.sjr.testutil.{readerToString, sqlArrayToSeq, toByteSeq, toUtf8String}
import org.sjr.{RowHandler, WrappedResultSet}

import java.sql.SQLXML
import java.{sql, util}
import java.util.concurrent.atomic.AtomicInteger


case class PgAllTypesRecord(
                             id: Long,
                             array: Seq[Int],
                             arrayOpt: Option[Seq[Int]],
                             bigDecimal: BigDecimal,
                             bigDecimalOpt: Option[BigDecimal],
                             boolean: Boolean,
                             booleanOpt: Option[Boolean],
                             byte: Byte,
                             byteOpt: Option[Byte],
                             byteSeqFromBinaryStream: Seq[Byte],
                             byteSeqFromBinaryStreamOpt: Option[Seq[Byte]],
                             byteSeqFromBytes: Seq[Byte],
                             byteSeqFromBytesOpt: Option[Seq[Byte]],
                             date: java.sql.Date,
                             dateOpt: Option[java.sql.Date],
                             double: Double,
                             doubleOpt: Option[Double],
                             float: Float,
                             floatOpt: Option[Float],
                             int: Int,
                             intOpt: Option[Int],
                             long: Long,
                             longOpt: Option[Long],
                             short: Short,
                             shortOpt: Option[Short],
                             sqlXml: String,
                             sqlXmlOpt: Option[String],
                             stringFromAsciiStream: String,
                             stringFromAsciiStreamOpt: Option[String],
                             stringFromCharacterStream: String,
                             stringFromCharacterStreamOpt: Option[String],
                             string: String,
                             stringOpt: Option[String],
                             time: java.sql.Time,
                             timeOpt: Option[java.sql.Time],
                             timestamp: java.sql.Timestamp,
                             timestampOpt: Option[java.sql.Timestamp]
                           ) extends AllTypesRecord[PgAllTypesRecord] {

  override def makeCopy(string: String): PgAllTypesRecord = this.copy(string = string)
}


class PgOrdinaryTableITCase extends OrdinaryTableITCase[PgAllTypesRecord] {

  override protected def getConnFactory(): support.ConnFactory = new TestContainerConnFactory(createPgContainer())

  override protected def tableName: String = "all_types_record"

  override protected def idColumnName: String = "id"

  override protected def stringColumnName: String = "string"

  override protected def stringOptColumnName: String = "string_opt"

  override protected def ddlForTable: String =
    """
      |CREATE TABLE all_types_record (
      |  id BIGINT PRIMARY KEY,
      |  array_value INT[] NOT NULL,
      |  array_opt INT[],
      |  big_decimal DECIMAL(10, 2) NOT NULL,
      |  big_decimal_opt DECIMAL(10, 2),
      |  boolean_value BOOLEAN NOT NULL,
      |  boolean_opt BOOLEAN,
      |  byte_value SMALLINT NOT NULL,
      |  byte_opt SMALLINT,
      |  byte_seq_from_binary_stream BYTEA NOT NULL,
      |  byte_seq_from_binary_stream_opt BYTEA,
      |  byte_seq_from_bytes BYTEA NOT NULL,
      |  byte_seq_from_bytes_opt BYTEA,
      |  date_value DATE NOT NULL,
      |  date_opt DATE,
      |  double_value DOUBLE PRECISION NOT NULL,
      |  double_opt DOUBLE PRECISION,
      |  float_value FLOAT NOT NULL,
      |  float_opt FLOAT,
      |  int_value INT NOT NULL,
      |  int_opt INT,
      |  long_value BIGINT NOT NULL,
      |  long_opt BIGINT,
      |  short_value SMALLINT NOT NULL,
      |  short_opt SMALLINT,
      |  sql_xml XML NOT NULL,
      |  sql_xml_opt XML,
      |  string_from_ascii_stream TEXT NOT NULL,
      |  string_from_ascii_stream_opt TEXT,
      |  string_from_character_stream TEXT NOT NULL,
      |  string_from_character_stream_opt TEXT,
      |  string VARCHAR(20) NOT NULL,
      |  string_opt VARCHAR(20),
      |  time_value TIME NOT NULL,
      |  time_opt TIME,
      |  timestamp_value TIMESTAMP NOT NULL,
      |  timestamp_opt TIMESTAMP
      |)
      |""".stripMargin


  override protected def insertSql: String =
    """
      |INSERT INTO all_types_record (
      |    id,
      |    array_value,
      |    array_opt,
      |    big_decimal,
      |    big_decimal_opt,
      |    boolean_value,
      |    boolean_opt,
      |    byte_value,
      |    byte_opt,
      |    byte_seq_from_binary_stream,
      |    byte_seq_from_binary_stream_opt,
      |    byte_seq_from_bytes,
      |    byte_seq_from_bytes_opt,
      |    date_value,
      |    date_opt,
      |    double_value,
      |    double_opt,
      |    float_value,
      |    float_opt,
      |    int_value,
      |    int_opt,
      |    long_value,
      |    long_opt,
      |    short_value,
      |    short_opt,
      |    sql_xml,
      |    sql_xml_opt,
      |    string,
      |    string_opt,
      |    string_from_ascii_stream,
      |    string_from_ascii_stream_opt,
      |    string_from_character_stream,
      |    string_from_character_stream_opt,
      |    time_value,
      |    time_opt,
      |    timestamp_value,
      |    timestamp_opt
      |) VALUES (
      |    ?,
      |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, XML(?), XML(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      |);
      |""".stripMargin


  override def recordWithTotalFields = PgAllTypesRecord(
    id = 111L,

    array = Seq[Int](1,2,3),
    arrayOpt = Some(Seq[Int](4, 5, 6)),
    bigDecimal = BigDecimal("123.45"),
    bigDecimalOpt = Some(BigDecimal("678.90")),
    boolean = true,
    booleanOpt = Some(false),
    byte = 120,
    byteOpt = Some(121),
    byteSeqFromBinaryStream = Seq[Byte](1, 2, 3),
    byteSeqFromBinaryStreamOpt = Some(Seq[Byte](4, 5, 6)),
    byteSeqFromBytes = Seq[Byte](7, 8, 9),
    byteSeqFromBytesOpt = Some(Seq[Byte](10, 11, 12)),
    date = java.sql.Date.valueOf("2024-01-01"),
    dateOpt = Some(java.sql.Date.valueOf("2024-02-02")),
    double = 7.89,
    doubleOpt = Some(10.11),
    float = 1.23f,
    floatOpt = Some(4.56f),
    int = 123,
    intOpt = Some(456),
    long = 789L,
    longOpt = Some(101112L),
    short = 1,
    shortOpt = Some(2),
    sqlXml = "<root>foo</root>",
    sqlXmlOpt = Some("<root>bar</root>"),
    stringFromAsciiStream = "abc",
    stringFromAsciiStreamOpt = Some("def"),
    stringFromCharacterStream = "aabbcc",
    stringFromCharacterStreamOpt = Some("ddeeff"),
    stringOpt = Some("stringOpt"),
    string = "string",
    time = java.sql.Time.valueOf("12:34:56"),
    timeOpt = Some(java.sql.Time.valueOf("01:23:45")),
    timestamp = java.sql.Timestamp.valueOf("2024-01-01 12:34:56"),
    timestampOpt = Some(java.sql.Timestamp.valueOf("2024-02-02 01:23:45"))
  )

  override def recordWithRequiredFields = PgAllTypesRecord(
    id = 222L,

    array = Seq[Int](1,2,3),
    arrayOpt = None,
    bigDecimal = BigDecimal("123.45"),
    bigDecimalOpt = None,
    boolean = true,
    booleanOpt = None,
    byte = 120,
    byteOpt = None,
    byteSeqFromBinaryStream = Seq[Byte](1, 2, 3),
    byteSeqFromBinaryStreamOpt = None,
    byteSeqFromBytes = Seq[Byte](7, 8, 9),
    byteSeqFromBytesOpt = None,
    date = java.sql.Date.valueOf("2024-01-01"),
    dateOpt = None,
    double = 7.89,
    doubleOpt = None,
    float = 1.23f,
    floatOpt = None,
    int = 123,
    intOpt = None,
    long = 789L,
    longOpt = None,
    short = 1,
    shortOpt = None,
    sqlXml = "<root>foo</root>",
    sqlXmlOpt = None,
    stringFromAsciiStream = "abc",
    stringFromAsciiStreamOpt = None,
    stringFromCharacterStream = "aabbcc",
    stringFromCharacterStreamOpt = None,
    string = "string",
    stringOpt = None,
    time = java.sql.Time.valueOf("12:34:56"),
    timeOpt = None,
    timestamp = java.sql.Timestamp.valueOf("2024-01-01 12:34:56"),
    timestampOpt = None
  )


  override protected def getRowHandler: RowHandler[PgAllTypesRecord] = new PgAllTypesRecordRowHandler

  override protected def getByIndexRowHandler: RowHandler[PgAllTypesRecord] = new PgAllTypesRecordByIndexRowHandler


  private class PgAllTypesRecordRowHandler extends RowHandler[PgAllTypesRecord] {

    override def handle(rs: WrappedResultSet): PgAllTypesRecord = PgAllTypesRecord(
      id = rs.getLong("ID"),

      array = sqlArrayToSeq[Int](rs.getArray("ARRAY_VALUE")),
      arrayOpt = rs.getArrayOpt("ARRAY_OPT").map(sqlArrayToSeq[Int](_)),
      bigDecimal = rs.getBigDecimal("BIG_DECIMAL"),
      bigDecimalOpt = rs.getBigDecimalOpt("BIG_DECIMAL_OPT"),
      boolean = rs.getBoolean("BOOLEAN_VALUE"),
      booleanOpt = rs.getBooleanOpt("BOOLEAN_OPT"),
      byte = rs.getByte("BYTE_VALUE"),
      byteOpt = rs.getByteOpt("BYTE_OPT"),
      byteSeqFromBinaryStream = toByteSeq(rs.getBinaryStream("BYTE_SEQ_FROM_BINARY_STREAM")),
      byteSeqFromBinaryStreamOpt = rs.getBinaryStreamOpt("BYTE_SEQ_FROM_BINARY_STREAM_OPT").map(toByteSeq),
      byteSeqFromBytes = rs.getBytes("BYTE_SEQ_FROM_BYTES").toSeq,
      byteSeqFromBytesOpt = rs.getBytesOpt("BYTE_SEQ_FROM_BYTES_OPT").map(_.toSeq),
      date = rs.getDate("DATE_VALUE"),
      dateOpt = rs.getDateOpt("DATE_OPT"),
      double = rs.getDouble("DOUBLE_VALUE"),
      doubleOpt = rs.getDoubleOpt("DOUBLE_OPT"),
      float = rs.getFloat("FLOAT_VALUE"),
      floatOpt = rs.getFloatOpt("FLOAT_OPT"),
      int = rs.getInt("INT_VALUE"),
      intOpt = rs.getIntOpt("INT_OPT"),
      long = rs.getLong("LONG_VALUE"),
      longOpt = rs.getLongOpt("LONG_OPT"),
      short = rs.getShort("SHORT_VALUE"),
      shortOpt = rs.getShortOpt("SHORT_OPT"),
      sqlXml = rs.getSQLXML("SQL_XML").getString,
      sqlXmlOpt = rs.getSQLXMLOpt("SQL_XML_OPT").flatMap(sx => Option(sx.getString)),
      stringFromAsciiStream = toUtf8String(rs.getAsciiStream("STRING_FROM_ASCII_STREAM")),
      stringFromAsciiStreamOpt = rs.getAsciiStreamOpt("STRING_FROM_ASCII_STREAM_OPT").map(toUtf8String),
      stringFromCharacterStream = readerToString(rs.getCharacterStream("STRING_FROM_CHARACTER_STREAM")),
      stringFromCharacterStreamOpt = rs.getCharacterStreamOpt("STRING_FROM_CHARACTER_STREAM_OPT").map(readerToString),
      string = rs.getString("STRING"),
      stringOpt = rs.getStringOpt("STRING_OPT"),
      time = rs.getTime("TIME_VALUE"),
      timeOpt = rs.getTimeOpt("TIME_OPT"),
      timestamp = rs.getTimestamp("TIMESTAMP_VALUE"),
      timestampOpt = rs.getTimestampOpt("TIMESTAMP_OPT")
    )
  }


  private class PgAllTypesRecordByIndexRowHandler extends RowHandler[PgAllTypesRecord] {

    override def handle(rs: WrappedResultSet): PgAllTypesRecord = {
      val i = new AtomicInteger(1)
      PgAllTypesRecord(
        id = rs.getLong(i.getAndIncrement),
        array = sqlArrayToSeq[Int](rs.getArray(i.getAndIncrement)),
        arrayOpt = rs.getArrayOpt(i.getAndIncrement).map(sqlArrayToSeq[Int](_)),
        bigDecimal = rs.getBigDecimal(i.getAndIncrement),
        bigDecimalOpt = rs.getBigDecimalOpt(i.getAndIncrement),
        boolean = rs.getBoolean(i.getAndIncrement),
        booleanOpt = rs.getBooleanOpt(i.getAndIncrement),
        byte = rs.getByte(i.getAndIncrement),
        byteOpt = rs.getByteOpt(i.getAndIncrement),
        byteSeqFromBinaryStream = toByteSeq(rs.getBinaryStream(i.getAndIncrement)),
        byteSeqFromBinaryStreamOpt = rs.getBinaryStreamOpt(i.getAndIncrement).map(toByteSeq),
        byteSeqFromBytes = rs.getBytes(i.getAndIncrement).toSeq,
        byteSeqFromBytesOpt = rs.getBytesOpt(i.getAndIncrement).map(_.toSeq),
        date = rs.getDate(i.getAndIncrement),
        dateOpt = rs.getDateOpt(i.getAndIncrement),
        double = rs.getDouble(i.getAndIncrement),
        doubleOpt = rs.getDoubleOpt(i.getAndIncrement),
        float = rs.getFloat(i.getAndIncrement),
        floatOpt = rs.getFloatOpt(i.getAndIncrement),
        int = rs.getInt(i.getAndIncrement),
        intOpt = rs.getIntOpt(i.getAndIncrement),
        long = rs.getLong(i.getAndIncrement),
        longOpt = rs.getLongOpt(i.getAndIncrement),
        short = rs.getShort(i.getAndIncrement),
        shortOpt = rs.getShortOpt(i.getAndIncrement),
        sqlXml = rs.getSQLXML(i.getAndIncrement).getString,
        sqlXmlOpt = rs.getSQLXMLOpt(i.getAndIncrement).flatMap(sx => Option(sx.getString)),
        stringFromAsciiStream = toUtf8String(rs.getAsciiStream(i.getAndIncrement)),
        stringFromAsciiStreamOpt = rs.getAsciiStreamOpt(i.getAndIncrement).map(toUtf8String),
        stringFromCharacterStream = readerToString(rs.getCharacterStream(i.getAndIncrement)),
        stringFromCharacterStreamOpt = rs.getCharacterStreamOpt(i.getAndIncrement).map(readerToString),
        string = rs.getString(i.getAndIncrement),
        stringOpt = rs.getStringOpt(i.getAndIncrement),
        time = rs.getTime(i.getAndIncrement),
        timeOpt = rs.getTimeOpt(i.getAndIncrement),
        timestamp = rs.getTimestamp(i.getAndIncrement),
        timestampOpt = rs.getTimestampOpt(i.getAndIncrement)
      )
    }
  }

  override protected def assertDataInDb(expected: PgAllTypesRecord, recordInDb: util.Map[String, Object]): Unit = {
    assertEquals(expected.id, recordInDb.get("ID"))

    assertEquals(expected.array, sqlArrayToSeq(recordInDb.get("ARRAY_VALUE").asInstanceOf[sql.Array]))
    assertEquals(expected.arrayOpt, Option(recordInDb.get("ARRAY_OPT")).map(arr => sqlArrayToSeq(arr.asInstanceOf[sql.Array])))
    assertEquals(expected.bigDecimal.bigDecimal, recordInDb.get("BIG_DECIMAL"))
    assertEquals(expected.bigDecimalOpt.map(_.bigDecimal), Option(recordInDb.get("BIG_DECIMAL_OPT")))
    assertEquals(expected.boolean, recordInDb.get("BOOLEAN_VALUE"))
    assertEquals(expected.booleanOpt, Option(recordInDb.get("BOOLEAN_OPT")))
    assertEquals(expected.byte.toInt, recordInDb.get("BYTE_VALUE"))
    assertEquals(expected.byteOpt.map(_.toInt), Option(recordInDb.get("BYTE_OPT")))
    assertEquals(expected.byteSeqFromBinaryStream, recordInDb.get("BYTE_SEQ_FROM_BINARY_STREAM").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(expected.byteSeqFromBinaryStreamOpt, Option(recordInDb.get("BYTE_SEQ_FROM_BINARY_STREAM_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(expected.byteSeqFromBytes, recordInDb.get("BYTE_SEQ_FROM_BYTES").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(expected.byteSeqFromBytesOpt, Option(recordInDb.get("BYTE_SEQ_FROM_BYTES_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(expected.date, recordInDb.get("DATE_VALUE"))
    assertEquals(expected.dateOpt, Option(recordInDb.get("DATE_OPT")))
    assertEquals(expected.double, recordInDb.get("DOUBLE_VALUE"))
    assertEquals(expected.doubleOpt, Option(recordInDb.get("DOUBLE_OPT")))
    assertEquals(expected.float.toDouble, recordInDb.get("FLOAT_VALUE"))
    assertEquals(expected.floatOpt.map(_.toDouble), Option(recordInDb.get("FLOAT_OPT")))
    assertEquals(expected.int, recordInDb.get("INT_VALUE"))
    assertEquals(expected.intOpt, Option(recordInDb.get("INT_OPT")))
    assertEquals(expected.long, recordInDb.get("LONG_VALUE"))
    assertEquals(expected.longOpt, Option(recordInDb.get("LONG_OPT")))
    assertEquals(expected.short.toInt, recordInDb.get("SHORT_VALUE"))
    assertEquals(expected.shortOpt.map(_.toInt), Option(recordInDb.get("SHORT_OPT")))
    assertEquals(expected.sqlXml, recordInDb.get("SQL_XML").asInstanceOf[SQLXML].getString)
    assertEquals(expected.sqlXmlOpt, Option(recordInDb.get("SQL_XML_OPT")).map(_.asInstanceOf[SQLXML].getString))
    assertEquals(expected.stringFromAsciiStream, recordInDb.get("STRING_FROM_ASCII_STREAM"))
    assertEquals(expected.stringFromAsciiStreamOpt, Option(recordInDb.get("STRING_FROM_ASCII_STREAM_OPT")))
    assertEquals(expected.stringFromCharacterStream, recordInDb.get("STRING_FROM_CHARACTER_STREAM"))
    assertEquals(expected.stringFromCharacterStreamOpt, Option(recordInDb.get("STRING_FROM_CHARACTER_STREAM_OPT")))
    assertEquals(expected.string, recordInDb.get("STRING"))
    assertEquals(expected.stringOpt, Option(recordInDb.get("STRING_OPT")))
    assertEquals(expected.time, recordInDb.get("TIME_VALUE"))
    assertEquals(expected.timeOpt, Option(recordInDb.get("TIME_OPT")))
    assertEquals(expected.timestamp, recordInDb.get("TIMESTAMP_VALUE"))
    assertEquals(expected.timestampOpt, Option(recordInDb.get("TIMESTAMP_OPT")))
  }


  override protected def recordToParams(record: PgAllTypesRecord) = {
    Seq[Any](
      record.id,

      record.array.toArray,
      record.arrayOpt.map(_.toArray),
      record.bigDecimal,
      record.bigDecimalOpt,
      record.boolean,
      record.booleanOpt,
      record.byte,
      record.byteOpt,
      record.byteSeqFromBinaryStream.toArray,
      record.byteSeqFromBinaryStreamOpt.map(_.toArray),
      record.byteSeqFromBytes.toArray,
      record.byteSeqFromBytesOpt.map(_.toArray),
      record.date,
      record.dateOpt,
      record.double,
      record.doubleOpt,
      record.float,
      record.floatOpt,
      record.int,
      record.intOpt,
      record.long,
      record.longOpt,
      record.short,
      record.shortOpt,
      record.sqlXml,
      record.sqlXmlOpt,
      record.string,
      record.stringOpt,
      record.stringFromAsciiStream,
      record.stringFromAsciiStreamOpt,
      record.stringFromCharacterStream,
      record.stringFromCharacterStreamOpt,
      record.time,
      record.timeOpt,
      record.timestamp,
      record.timestampOpt
    )
  }

}