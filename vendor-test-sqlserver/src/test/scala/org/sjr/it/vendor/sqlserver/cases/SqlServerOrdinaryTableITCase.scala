package org.sjr.it.vendor.sqlServer.cases

import org.junit.jupiter.api.Assertions.assertEquals
import org.sjr.it.vendor.common.cases.{AllTypesRecord, OrdinaryTableITCase}
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory}
import org.sjr.it.vendor.sqlServer.cases
import org.sjr.it.vendor.sqlserver.support.createSqlServerContainer
import org.sjr.testutil.{readerToString, streamToByteSeq, toUtf8String}
import org.sjr.{PreparedStatementSetterParam, RowHandler, WrappedResultSet}

import java.sql.{Connection, PreparedStatement}
import java.util
import java.util.concurrent.atomic.AtomicInteger


case class SqlServerAllTypesRecord(
                                    id: Long,

                                    bigDecimal: BigDecimal,
                                    bigDecimalOpt: Option[BigDecimal],
                                    boolean: Boolean,
                                    booleanOpt: Option[Boolean],
                                    byte: Byte,
                                    byteOpt: Option[Byte],
                                    byteSeqFromBinaryStream: Seq[Byte],
                                    byteSeqFromBinaryStreamOpt: Option[Seq[Byte]],
                                    byteSeqFromBlob: Seq[Byte],
                                    byteSeqFromBlobOpt: Option[Seq[Byte]],
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
                                    nString: String,
                                    nStringOpt: Option[String],
                                    short: Short,
                                    shortOpt: Option[Short],
                                    sqlXml: String,
                                    sqlXmlOpt: Option[String],
                                    stringFromAsciiStream: String,
                                    stringFromAsciiStreamOpt: Option[String],
                                    stringFromCharacterStream: String,
                                    stringFromCharacterStreamOpt: Option[String],
                                    stringFromClob: String,
                                    stringFromClobOpt: Option[String],
                                    stringFromNCharacterStream: String,
                                    stringFromNCharacterStreamOpt: Option[String],
                                    stringFromNClob: String,
                                    stringFromNClobOpt: Option[String],
                                    string: String,
                                    stringOpt: Option[String],
                                    time: java.sql.Time,
                                    timeOpt: Option[java.sql.Time],
                                    timestamp: java.sql.Timestamp,
                                    timestampOpt: Option[java.sql.Timestamp]
                                  ) extends AllTypesRecord[SqlServerAllTypesRecord] {

  override def makeCopy(string: String): SqlServerAllTypesRecord = this.copy(string = string)
}

class SqlServerSetBytesParam(bytesOpt: Option[Array[Byte]]) extends PreparedStatementSetterParam {
  override def doSet(stmt: PreparedStatement, index: Int): Unit = {
    bytesOpt match {
      case Some(bytes) => stmt.setBytes(index, bytes)
      case None => stmt.setBytes(index, None.orNull)
    }
  }
}

class SqlServerOrdinaryTableITCase extends OrdinaryTableITCase[SqlServerAllTypesRecord] {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createSqlServerContainer())

  override protected def tableName: String = "all_types_record"

  override protected def idColumnName: String = "id"

  override protected def stringColumnName: String = "string"

  override protected def stringOptColumnName: String = "string_opt"

  override protected def ddlsForTable: Seq[String]  = Seq(
    """
      |CREATE TABLE all_types_record (
      |  id BIGINT PRIMARY KEY,
      |
      |  big_decimal DECIMAL(38, 2) NOT NULL,
      |  big_decimal_opt DECIMAL(38, 2),
      |  boolean_value BIT NOT NULL,
      |  boolean_opt BIT,
      |  byte_value TINYINT NOT NULL,
      |  byte_opt TINYINT,
      |  byte_seq_from_binary_stream VARBINARY(100) NOT NULL,
      |  byte_seq_from_binary_stream_opt VARBINARY(100),
      |  byte_seq_from_blob VARBINARY(MAX) NOT NULL,
      |  byte_seq_from_blob_opt VARBINARY(MAX),
      |  byte_seq_from_bytes VARBINARY(100) NOT NULL,
      |  byte_seq_from_bytes_opt VARBINARY(100),
      |  date_value DATE NOT NULL,
      |  date_opt DATE,
      |  double_value FLOAT NOT NULL,
      |  double_opt FLOAT,
      |  float_value REAL NOT NULL,
      |  float_opt REAL,
      |  int_value INT NOT NULL,
      |  int_opt INT,
      |  long_value BIGINT NOT NULL,
      |  long_opt BIGINT,
      |  nstring NVARCHAR(2) NOT NULL,
      |  nstring_opt NVARCHAR(2),
      |  short_value SMALLINT NOT NULL,
      |  short_opt SMALLINT,
      |  sql_xml XML NOT NULL,
      |  sql_xml_opt XML,
      |  string_from_ascii_stream TEXT NOT NULL,
      |  string_from_ascii_stream_opt TEXT,
      |  string_from_character_stream TEXT NOT NULL,
      |  string_from_character_stream_opt TEXT,
      |  string_from_clob TEXT NOT NULL,
      |  string_from_clob_opt TEXT,
      |  string_from_ncharacter_stream NVARCHAR(2) NOT NULL,
      |  string_from_ncharacter_stream_opt NVARCHAR(2),
      |  string_from_nclob NTEXT NOT NULL,
      |  string_from_nclob_opt NTEXT,
      |  string VARCHAR(20) NOT NULL,
      |  string_opt VARCHAR(20),
      |  time_value TIME NOT NULL,
      |  time_opt TIME,
      |  timestamp_value DATETIME NOT NULL,
      |  timestamp_opt DATETIME
      |)
      |""".stripMargin)


  override protected def insertSql: String =
    """
      |INSERT INTO all_types_record (
      |    id,
      |
      |    big_decimal,
      |    big_decimal_opt,
      |    boolean_value,
      |    boolean_opt,
      |    byte_value,
      |    byte_opt,
      |    byte_seq_from_binary_stream,
      |    byte_seq_from_binary_stream_opt,
      |    byte_seq_from_blob,
      |    byte_seq_from_blob_opt,
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
      |    nstring,
      |    nstring_opt,
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
      |    string_from_clob,
      |    string_from_clob_opt,
      |    string_from_ncharacter_stream,
      |    string_from_ncharacter_stream_opt,
      |    string_from_nclob,
      |    string_from_nclob_opt,
      |    time_value,
      |    time_opt,
      |    timestamp_value,
      |    timestamp_opt
      |) VALUES (
      |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      |)
      |""".stripMargin


  override def recordWithTotalFields = SqlServerAllTypesRecord(
    id = 111L,

    bigDecimal = BigDecimal("123.45"),
    bigDecimalOpt = Some(BigDecimal("678.90")),
    boolean = true,
    booleanOpt = Some(false),
    byte = 120,
    byteOpt = Some(121),
    byteSeqFromBinaryStream = Seq[Byte](1, 2, 3),
    byteSeqFromBinaryStreamOpt = Some(Seq[Byte](4, 5, 6)),
    byteSeqFromBlob = Seq[Byte](13, 14, 15),
    byteSeqFromBlobOpt = Some(Seq[Byte](16, 17, 18)),
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
    nString = "水星",
    nStringOpt = Some("火星"),
    short = 1,
    shortOpt = Some(2),
    sqlXml = "<root>foo</root>",
    sqlXmlOpt = Some("<root>bar</root>"),
    stringFromAsciiStream = "abc",
    stringFromAsciiStreamOpt = Some("def"),
    stringFromCharacterStream = "aabbcc",
    stringFromCharacterStreamOpt = Some("ddeeff"),
    stringFromClob = "ghi",
    stringFromClobOpt = Some("jkl"),
    stringFromNCharacterStream = "金星",
    stringFromNCharacterStreamOpt = Some("木星"),
    stringFromNClob = "地球",
    stringFromNClobOpt = Some("月亮"),
    stringOpt = Some("stringOpt"),
    string = "string",
    time = java.sql.Time.valueOf("12:34:56"),
    timeOpt = Some(java.sql.Time.valueOf("01:23:45")),
    timestamp = java.sql.Timestamp.valueOf("2024-01-01 12:34:56"),
    timestampOpt = Some(java.sql.Timestamp.valueOf("2024-02-02 01:23:45"))
  )

  override def recordWithRequiredFields = SqlServerAllTypesRecord(
    id = 222L,

    bigDecimal = BigDecimal("123.45"),
    bigDecimalOpt = None,
    boolean = true,
    booleanOpt = None,
    byte = 120,
    byteOpt = None,
    byteSeqFromBinaryStream = Seq[Byte](1, 2, 3),
    byteSeqFromBinaryStreamOpt = None,
    byteSeqFromBlob = Seq[Byte](13, 14, 15),
    byteSeqFromBlobOpt = None,
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
    nString = "水星",
    nStringOpt = None,
    short = 1,
    shortOpt = None,
    sqlXml = "<root>foo</root>",
    sqlXmlOpt = None,
    stringFromAsciiStream = "abc",
    stringFromAsciiStreamOpt = None,
    stringFromCharacterStream = "aabbcc",
    stringFromCharacterStreamOpt = None,
    stringFromClob = "ghi",
    stringFromClobOpt = None,
    stringFromNCharacterStream = "金星",
    stringFromNCharacterStreamOpt = None,
    stringFromNClob = "地球",
    stringFromNClobOpt = None,
    string = "string",
    stringOpt = None,
    time = java.sql.Time.valueOf("12:34:56"),
    timeOpt = None,
    timestamp = java.sql.Timestamp.valueOf("2024-01-01 12:34:56"),
    timestampOpt = None
  )

  override protected def getRowHandler: RowHandler[SqlServerAllTypesRecord] = new SqlServerAllTypesRecordRowHandler

  override protected def getByIndexRowHandler: RowHandler[SqlServerAllTypesRecord] = new SqlServerAllTypesRecordByIndexRowHandler


  private class SqlServerAllTypesRecordRowHandler extends RowHandler[SqlServerAllTypesRecord] {

    override def handle(rs: WrappedResultSet): SqlServerAllTypesRecord = cases.SqlServerAllTypesRecord(
      id = rs.getScalaLong("ID"),

      bigDecimal = rs.getBigDecimal("BIG_DECIMAL"),
      bigDecimalOpt = rs.getBigDecimalOpt("BIG_DECIMAL_OPT"),
      boolean = rs.getScalaBoolean("BOOLEAN_VALUE"),
      booleanOpt = rs.getScalaBooleanOpt("BOOLEAN_OPT"),
      byte = rs.getScalaByte("BYTE_VALUE"),
      byteOpt = rs.getScalaByteOpt("BYTE_OPT"),
      byteSeqFromBinaryStream = streamToByteSeq(rs.getBinaryStream("BYTE_SEQ_FROM_BINARY_STREAM")),
      byteSeqFromBinaryStreamOpt = rs.getBinaryStreamOpt("BYTE_SEQ_FROM_BINARY_STREAM_OPT").map(streamToByteSeq),
      byteSeqFromBlob = streamToByteSeq(rs.getBlob("BYTE_SEQ_FROM_BLOB").getBinaryStream),
      byteSeqFromBlobOpt = rs.getBlobOpt("BYTE_SEQ_FROM_BLOB_OPT").map(blob => streamToByteSeq(blob.getBinaryStream)),
      byteSeqFromBytes = rs.getBytes("BYTE_SEQ_FROM_BYTES").toSeq,
      byteSeqFromBytesOpt = rs.getBytesOpt("BYTE_SEQ_FROM_BYTES_OPT").map(_.toSeq),
      date = rs.getDate("DATE_VALUE"),
      dateOpt = rs.getDateOpt("DATE_OPT"),
      double = rs.getScalaDouble("DOUBLE_VALUE"),
      doubleOpt = rs.getScalaDoubleOpt("DOUBLE_OPT"),
      float = rs.getScalaFloat("FLOAT_VALUE"),
      floatOpt = rs.getScalaFloatOpt("FLOAT_OPT"),
      int = rs.getScalaInt("INT_VALUE"),
      intOpt = rs.getScalaIntOpt("INT_OPT"),
      long = rs.getScalaLong("LONG_VALUE"),
      longOpt = rs.getScalaLongOpt("LONG_OPT"),
      nString = rs.getNString("NSTRING"),
      nStringOpt = rs.getNStringOpt("NSTRING_OPT"),
      short = rs.getScalaShort("SHORT_VALUE"),
      shortOpt = rs.getScalaShortOpt("SHORT_OPT"),
      sqlXml = rs.getSQLXML("SQL_XML").getString,
      sqlXmlOpt = rs.getSQLXMLOpt("SQL_XML_OPT").map(_.getString),
      stringFromAsciiStream = toUtf8String(rs.getAsciiStream("STRING_FROM_ASCII_STREAM")),
      stringFromAsciiStreamOpt = rs.getAsciiStreamOpt("STRING_FROM_ASCII_STREAM_OPT").map(toUtf8String),
      stringFromCharacterStream = readerToString(rs.getCharacterStream("STRING_FROM_CHARACTER_STREAM")),
      stringFromCharacterStreamOpt = rs.getCharacterStreamOpt("STRING_FROM_CHARACTER_STREAM_OPT").map(readerToString),
      stringFromClob = readerToString(rs.getClob("STRING_FROM_CLOB").getCharacterStream),
      stringFromClobOpt = rs.getClobOpt("STRING_FROM_CLOB_OPT").map(clob => readerToString(clob.getCharacterStream)),
      stringFromNCharacterStream = readerToString(rs.getNCharacterStream("STRING_FROM_NCHARACTER_STREAM")),
      stringFromNCharacterStreamOpt = rs.getNCharacterStreamOpt("STRING_FROM_NCHARACTER_STREAM_OPT").map(readerToString),
      stringFromNClob = readerToString(rs.getNClob("STRING_FROM_NCLOB").getCharacterStream),
      stringFromNClobOpt = rs.getNClobOpt("STRING_FROM_NCLOB_OPT").map(clob => readerToString(clob.getCharacterStream)),
      string = rs.getString("STRING"),
      stringOpt = rs.getStringOpt("STRING_OPT"),
      time = rs.getTime("TIME_VALUE"),
      timeOpt = rs.getTimeOpt("TIME_OPT"),
      timestamp = rs.getTimestamp("TIMESTAMP_VALUE"),
      timestampOpt = rs.getTimestampOpt("TIMESTAMP_OPT")
    )
  }


  private class SqlServerAllTypesRecordByIndexRowHandler extends RowHandler[SqlServerAllTypesRecord] {

    override def handle(rs: WrappedResultSet): SqlServerAllTypesRecord = {
      val i = new AtomicInteger(1)
      cases.SqlServerAllTypesRecord(
        id = rs.getScalaLong(i.getAndIncrement),

        bigDecimal = rs.getBigDecimal(i.getAndIncrement),
        bigDecimalOpt = rs.getBigDecimalOpt(i.getAndIncrement),
        boolean = rs.getScalaBoolean(i.getAndIncrement),
        booleanOpt = rs.getScalaBooleanOpt(i.getAndIncrement),
        byte = rs.getScalaByte(i.getAndIncrement),
        byteOpt = rs.getScalaByteOpt(i.getAndIncrement),
        byteSeqFromBinaryStream = streamToByteSeq(rs.getBinaryStream(i.getAndIncrement)),
        byteSeqFromBinaryStreamOpt = rs.getBinaryStreamOpt(i.getAndIncrement).map(streamToByteSeq),
        byteSeqFromBlob = streamToByteSeq(rs.getBlob(i.getAndIncrement).getBinaryStream),
        byteSeqFromBlobOpt = rs.getBlobOpt(i.getAndIncrement).map(blob => streamToByteSeq(blob.getBinaryStream)),
        byteSeqFromBytes = rs.getBytes(i.getAndIncrement).toSeq,
        byteSeqFromBytesOpt = rs.getBytesOpt(i.getAndIncrement).map(_.toSeq),
        date = rs.getDate(i.getAndIncrement),
        dateOpt = rs.getDateOpt(i.getAndIncrement),
        double = rs.getScalaDouble(i.getAndIncrement),
        doubleOpt = rs.getScalaDoubleOpt(i.getAndIncrement),
        float = rs.getScalaFloat(i.getAndIncrement),
        floatOpt = rs.getScalaFloatOpt(i.getAndIncrement),
        int = rs.getScalaInt(i.getAndIncrement),
        intOpt = rs.getScalaIntOpt(i.getAndIncrement),
        long = rs.getScalaLong(i.getAndIncrement),
        longOpt = rs.getScalaLongOpt(i.getAndIncrement),
        nString = rs.getNString(i.getAndIncrement),
        nStringOpt = rs.getNStringOpt(i.getAndIncrement),
        short = rs.getScalaShort(i.getAndIncrement),
        shortOpt = rs.getScalaShortOpt(i.getAndIncrement),
        sqlXml = rs.getSQLXML(i.getAndIncrement).getString,
        sqlXmlOpt = rs.getSQLXMLOpt(i.getAndIncrement).map(_.getString),
        stringFromAsciiStream = toUtf8String(rs.getAsciiStream(i.getAndIncrement)),
        stringFromAsciiStreamOpt = rs.getAsciiStreamOpt(i.getAndIncrement).map(toUtf8String),
        stringFromCharacterStream = readerToString(rs.getCharacterStream(i.getAndIncrement)),
        stringFromCharacterStreamOpt = rs.getCharacterStreamOpt(i.getAndIncrement).map(readerToString),
        stringFromClob = readerToString(rs.getClob(i.getAndIncrement).getCharacterStream),
        stringFromClobOpt = rs.getClobOpt(i.getAndIncrement).map(clob => readerToString(clob.getCharacterStream)),
        stringFromNCharacterStream = readerToString(rs.getNCharacterStream(i.getAndIncrement)),
        stringFromNCharacterStreamOpt = rs.getNCharacterStreamOpt(i.getAndIncrement).map(readerToString),
        stringFromNClob = readerToString(rs.getNClob(i.getAndIncrement).getCharacterStream),
        stringFromNClobOpt = rs.getNClobOpt(i.getAndIncrement).map(clob => readerToString(clob.getCharacterStream)),
        string = rs.getString(i.getAndIncrement),
        stringOpt = rs.getStringOpt(i.getAndIncrement),
        time = rs.getTime(i.getAndIncrement),
        timeOpt = rs.getTimeOpt(i.getAndIncrement),
        timestamp = rs.getTimestamp(i.getAndIncrement),
        timestampOpt = rs.getTimestampOpt(i.getAndIncrement)
      )
    }
  }

  override protected def assertDataInDb(expected: SqlServerAllTypesRecord, recordInDb: util.Map[String, Object]): Unit = {
    assertEquals(expected.id, recordInDb.get("ID"))

    assertEquals(expected.bigDecimal.bigDecimal, recordInDb.get("BIG_DECIMAL"))
    assertEquals(expected.bigDecimalOpt.map(_.bigDecimal), Option(recordInDb.get("BIG_DECIMAL_OPT")))
    assertEquals(expected.boolean, recordInDb.get("BOOLEAN_VALUE"))
    assertEquals(expected.booleanOpt, Option(recordInDb.get("BOOLEAN_OPT")))
    assertEquals(expected.byte.toShort, recordInDb.get("BYTE_VALUE"))
    assertEquals(expected.byteOpt.map(_.toShort), Option(recordInDb.get("BYTE_OPT")))
    assertEquals(expected.byteSeqFromBinaryStream, recordInDb.get("BYTE_SEQ_FROM_BINARY_STREAM").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(expected.byteSeqFromBinaryStreamOpt, Option(recordInDb.get("BYTE_SEQ_FROM_BINARY_STREAM_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(expected.byteSeqFromBlob, recordInDb.get("BYTE_SEQ_FROM_BLOB").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(expected.byteSeqFromBlobOpt, Option(recordInDb.get("BYTE_SEQ_FROM_BLOB_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(expected.byteSeqFromBytes, recordInDb.get("BYTE_SEQ_FROM_BYTES").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(expected.byteSeqFromBytesOpt, Option(recordInDb.get("BYTE_SEQ_FROM_BYTES_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(expected.date, recordInDb.get("DATE_VALUE"))
    assertEquals(expected.dateOpt, Option(recordInDb.get("DATE_OPT")))
    assertEquals(expected.double, recordInDb.get("DOUBLE_VALUE"))
    assertEquals(expected.doubleOpt, Option(recordInDb.get("DOUBLE_OPT")))
    assertEquals(expected.float, recordInDb.get("FLOAT_VALUE"))
    assertEquals(expected.floatOpt, Option(recordInDb.get("FLOAT_OPT")))
    assertEquals(expected.int, recordInDb.get("INT_VALUE"))
    assertEquals(expected.intOpt, Option(recordInDb.get("INT_OPT")))
    assertEquals(expected.long, recordInDb.get("LONG_VALUE"))
    assertEquals(expected.longOpt, Option(recordInDb.get("LONG_OPT")))
    assertEquals(expected.nString, recordInDb.get("NSTRING"))
    assertEquals(expected.nStringOpt, Option(recordInDb.get("NSTRING_OPT")))
    assertEquals(expected.short, recordInDb.get("SHORT_VALUE"))
    assertEquals(expected.shortOpt, Option(recordInDb.get("SHORT_OPT")))
    assertEquals(expected.sqlXml, recordInDb.get("SQL_XML"))
    assertEquals(expected.sqlXmlOpt, Option(recordInDb.get("SQL_XML_OPT")))
    assertEquals(expected.stringFromAsciiStream, recordInDb.get("STRING_FROM_ASCII_STREAM"))
    assertEquals(expected.stringFromAsciiStreamOpt, Option(recordInDb.get("STRING_FROM_ASCII_STREAM_OPT")))
    assertEquals(expected.stringFromCharacterStream, recordInDb.get("STRING_FROM_CHARACTER_STREAM"))
    assertEquals(expected.stringFromCharacterStreamOpt, Option(recordInDb.get("STRING_FROM_CHARACTER_STREAM_OPT")))
    assertEquals(expected.stringFromClob, recordInDb.get("STRING_FROM_CLOB"))
    assertEquals(expected.stringFromClobOpt, Option(recordInDb.get("STRING_FROM_CLOB_OPT")))
    assertEquals(expected.stringFromNCharacterStream, recordInDb.get("STRING_FROM_NCHARACTER_STREAM"))
    assertEquals(expected.stringFromNCharacterStreamOpt, Option(recordInDb.get("STRING_FROM_NCHARACTER_STREAM_OPT")))
    assertEquals(expected.stringFromNClob, recordInDb.get("STRING_FROM_NCLOB"))
    assertEquals(expected.stringFromNClobOpt, Option(recordInDb.get("STRING_FROM_NCLOB_OPT")))
    assertEquals(expected.string, recordInDb.get("STRING"))
    assertEquals(expected.stringOpt, Option(recordInDb.get("STRING_OPT")))
    assertEquals(expected.time, recordInDb.get("TIME_VALUE"))
    assertEquals(expected.timeOpt, Option(recordInDb.get("TIME_OPT")))
    assertEquals(expected.timestamp, recordInDb.get("TIMESTAMP_VALUE"))
    assertEquals(expected.timestampOpt, Option(recordInDb.get("TIMESTAMP_OPT")))
  }

  override protected def recordToParams(record: SqlServerAllTypesRecord)(implicit conn: Connection) = {
    Seq[Any](
      record.id,

      record.bigDecimal,
      record.bigDecimalOpt,
      record.boolean,
      record.booleanOpt,
      record.byte,
      record.byteOpt,
      new SqlServerSetBytesParam(Some(record.byteSeqFromBinaryStream.toArray)),
      new SqlServerSetBytesParam(record.byteSeqFromBinaryStreamOpt.map(_.toArray)),
      new SqlServerSetBytesParam(Some(record.byteSeqFromBlob.toArray)),
      new SqlServerSetBytesParam(record.byteSeqFromBlobOpt.map(_.toArray)),
      new SqlServerSetBytesParam(Some(record.byteSeqFromBytes.toArray)),
      new SqlServerSetBytesParam(record.byteSeqFromBytesOpt.map(_.toArray)),
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
      record.nString,
      record.nStringOpt,
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
      record.stringFromClob,
      record.stringFromClobOpt,
      record.stringFromNCharacterStream,
      record.stringFromNCharacterStreamOpt,
      record.stringFromNClob,
      record.stringFromNClobOpt,
      record.time,
      record.timeOpt,
      record.timestamp,
      record.timestampOpt
    )
  }

}
