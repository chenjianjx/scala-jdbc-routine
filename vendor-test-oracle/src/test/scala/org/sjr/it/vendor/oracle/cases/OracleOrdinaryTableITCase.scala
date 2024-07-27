package org.sjr.it.vendor.oracle.cases

import org.junit.jupiter.api.Assertions.assertEquals
import org.sjr.it.vendor.common.cases.{AllTypesRecord, OrdinaryTableITCase}
import org.sjr.it.vendor.common.support.{ConnFactory, TestContainerConnFactory}
import org.sjr.it.vendor.oracle.cases
import org.sjr.it.vendor.oracle.support.createOracleContainer
import org.sjr.testutil.{blobToByteSeq, readerToString, sqlArrayToSeq, streamToByteSeq, toUtf8String}
import org.sjr.{PreparedStatementSetterParam, RowHandler, WrappedResultSet}

import java.net.URL
import java.sql.{Array, Blob, Clob, Connection, NClob, PreparedStatement, Timestamp, Types}
import java.util.concurrent.atomic.AtomicInteger
import java.{sql, util}


case class OracleAllTypesRecord(
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
                                 timestampOpt: Option[java.sql.Timestamp],
                                 url: URL,
                                 urlOpt: Option[URL]
                               ) extends AllTypesRecord[OracleAllTypesRecord] {

  override def makeCopy(string: String): OracleAllTypesRecord = this.copy(string = string)
}

class OracleSetArrayParam(arrayOpt: Option[Array], arrayTypeName: String) extends PreparedStatementSetterParam {
  override def doSet(stmt: PreparedStatement, index: Int): Unit =
    arrayOpt match {
      case Some(array) => stmt.setArray(index, array)
      case None => stmt.setNull(index, Types.ARRAY, arrayTypeName)
    }
}

class OracleOrdinaryTableITCase extends OrdinaryTableITCase[OracleAllTypesRecord] {

  override protected def getConnFactory(): ConnFactory = new TestContainerConnFactory(createOracleContainer())

  override protected def tableName: String = "all_types_record"

  override protected def idColumnName: String = "id"

  override protected def stringColumnName: String = "string"

  override protected def stringOptColumnName: String = "string_opt"

  override protected def ddlsForTable: Seq[String] = Seq(
    "CREATE TYPE INT_ARRAY_10 AS VARRAY(10) OF NUMBER(10)",
    """
      |CREATE TABLE all_types_record (
      |  id NUMBER(19) PRIMARY KEY,
      |  array_value INT_ARRAY_10 NOT NULL,
      |  array_opt INT_ARRAY_10,
      |  big_decimal NUMBER(38,2) NOT NULL,
      |  big_decimal_opt NUMBER(38,2),
      |  boolean_value NUMBER(1) NOT NULL,
      |  boolean_opt NUMBER(1),
      |  byte_value NUMBER(3) NOT NULL,
      |  byte_opt NUMBER(3),
      |  byte_seq_from_binary_stream RAW(100) NOT NULL,
      |  byte_seq_from_binary_stream_opt RAW(100),
      |  byte_seq_from_blob BLOB NOT NULL,
      |  byte_seq_from_blob_opt BLOB,
      |  byte_seq_from_bytes RAW(100) NOT NULL,
      |  byte_seq_from_bytes_opt RAW(100),
      |  date_value DATE NOT NULL,
      |  date_opt DATE,
      |  double_value NUMBER(19,4) NOT NULL,
      |  double_opt NUMBER(19,4),
      |  float_value NUMBER(19,4) NOT NULL,
      |  float_opt NUMBER(19,4),
      |  int_value NUMBER(10) NOT NULL,
      |  int_opt NUMBER(10),
      |  long_value NUMBER(19) NOT NULL,
      |  long_opt NUMBER(19),
      |  nstring NVARCHAR2(2) NOT NULL,
      |  nstring_opt NVARCHAR2(2),
      |  short_value NUMBER(5) NOT NULL,
      |  short_opt NUMBER(5),
      |  sql_xml XMLTYPE NOT NULL,
      |  sql_xml_opt XMLTYPE,
      |  string_from_ascii_stream CLOB NOT NULL,
      |  string_from_ascii_stream_opt CLOB,
      |  string_from_character_stream CLOB NOT NULL,
      |  string_from_character_stream_opt CLOB,
      |  string_from_clob CLOB NOT NULL,
      |  string_from_clob_opt CLOB,
      |  string_from_ncharacter_stream NVARCHAR2(2) NOT NULL,
      |  string_from_ncharacter_stream_opt NVARCHAR2(2),
      |  string_from_nclob NCLOB NOT NULL,
      |  string_from_nclob_opt NCLOB,
      |  string VARCHAR(20) NOT NULL,
      |  string_opt VARCHAR(20),
      |  time_value DATE NOT NULL,
      |  time_opt DATE,
      |  timestamp_value TIMESTAMP NOT NULL,
      |  timestamp_opt TIMESTAMP,
      |  url_value VARCHAR(2083) NOT NULL,
      |  url_opt VARCHAR(2083)
      |)
      |""".stripMargin
  )

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
      |    timestamp_opt,
      |    url_value,
      |    url_opt
      |) VALUES (
      |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      |)
      |""".stripMargin


  override def recordWithTotalFields = OracleAllTypesRecord(
    id = 111L,
    array = Seq[Int](1, 2, 3),
    arrayOpt = Some(Seq[Int](4, 5, 6)),
    bigDecimal = BigDecimal("123.45"),
    bigDecimalOpt = Some(BigDecimal("678.9")),
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
    timestampOpt = Some(java.sql.Timestamp.valueOf("2024-02-02 01:23:45")),
    url = new URL("https://www.foo.com"),
    urlOpt = Some(new URL("http://localhost:8080"))
  )

  override def recordWithRequiredFields = OracleAllTypesRecord(
    id = 222L,

    array = Seq[Int](1, 2, 3),
    arrayOpt = None,
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
    timestampOpt = None,
    url = new URL("https://www.foo.com"),
    urlOpt = None

  )

  override protected def getRowHandler: RowHandler[OracleAllTypesRecord] = new OracleAllTypesRecordRowHandler

  override protected def getByIndexRowHandler: RowHandler[OracleAllTypesRecord] = new OracleAllTypesRecordByIndexRowHandler


  private class OracleAllTypesRecordRowHandler extends RowHandler[OracleAllTypesRecord] {

    override def handle(rs: WrappedResultSet): OracleAllTypesRecord = {
      cases.OracleAllTypesRecord(
        id = rs.getScalaLong("ID"),
        array = sqlArrayToIntSeq(rs.getArray("ARRAY_VALUE")),
        arrayOpt = rs.getArrayOpt("ARRAY_OPT").map(sqlArrayToIntSeq(_)),
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
        sqlXml = rs.getSQLXML("SQL_XML").getString.trim,
        sqlXmlOpt = rs.getSQLXMLOpt("SQL_XML_OPT").map(_.getString.trim),
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
        timestampOpt = rs.getTimestampOpt("TIMESTAMP_OPT"),
        url = rs.getURL("URL_VALUE"),
        urlOpt = rs.getURLOpt("URL_OPT")
      )
    }
  }


  private class OracleAllTypesRecordByIndexRowHandler extends RowHandler[OracleAllTypesRecord] {

    override def handle(rs: WrappedResultSet): OracleAllTypesRecord = {
      val i = new AtomicInteger(1)
      cases.OracleAllTypesRecord(
        id = rs.getScalaLong(i.getAndIncrement),
        array = sqlArrayToIntSeq(rs.getArray(i.getAndIncrement)),
        arrayOpt = rs.getArrayOpt(i.getAndIncrement).map(sqlArrayToIntSeq(_)),
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
        sqlXml = rs.getSQLXML(i.getAndIncrement).getString.trim,
        sqlXmlOpt = rs.getSQLXMLOpt(i.getAndIncrement).map(_.getString.trim),
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
        timestampOpt = rs.getTimestampOpt(i.getAndIncrement),
        url = rs.getURL(i.getAndIncrement),
        urlOpt = rs.getURLOpt(i.getAndIncrement)
      )
    }
  }

  override protected def assertDataInDb(expected: OracleAllTypesRecord, recordInDb: util.Map[String, Object]): Unit = {
    assertEquals(expected.id, recordInDb.get("ID").asInstanceOf[java.math.BigDecimal].longValue())
    assertEquals(expected.array, sqlArrayToIntSeq(recordInDb.get("ARRAY_VALUE").asInstanceOf[sql.Array]))
    assertEquals(expected.arrayOpt, Option(recordInDb.get("ARRAY_OPT")).map(arr => sqlArrayToIntSeq(arr.asInstanceOf[sql.Array])))
    assertEquals(expected.bigDecimal.bigDecimal, recordInDb.get("BIG_DECIMAL"))
    assertEquals(expected.bigDecimalOpt.map(_.bigDecimal), Option(recordInDb.get("BIG_DECIMAL_OPT")))
    assertEquals(expected.boolean, recordInDb.get("BOOLEAN_VALUE").asInstanceOf[java.math.BigDecimal].intValue() == 1)
    assertEquals(expected.booleanOpt, Option(recordInDb.get("BOOLEAN_OPT")).map(_.asInstanceOf[java.math.BigDecimal].intValue() == 1))
    assertEquals(expected.byte.toInt, recordInDb.get("BYTE_VALUE").asInstanceOf[java.math.BigDecimal].intValue())
    assertEquals(expected.byteOpt.map(_.toInt), Option(recordInDb.get("BYTE_OPT")).map(_.asInstanceOf[java.math.BigDecimal].intValue()))
    assertEquals(expected.byteSeqFromBinaryStream, recordInDb.get("BYTE_SEQ_FROM_BINARY_STREAM").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(expected.byteSeqFromBinaryStreamOpt, Option(recordInDb.get("BYTE_SEQ_FROM_BINARY_STREAM_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(expected.byteSeqFromBlob, blobToByteSeq(recordInDb.get("BYTE_SEQ_FROM_BLOB").asInstanceOf[Blob]))
    assertEquals(expected.byteSeqFromBlobOpt, Option(recordInDb.get("BYTE_SEQ_FROM_BLOB_OPT")).map(obj => blobToByteSeq(obj.asInstanceOf[Blob])))
    assertEquals(expected.byteSeqFromBytes, recordInDb.get("BYTE_SEQ_FROM_BYTES").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(expected.byteSeqFromBytesOpt, Option(recordInDb.get("BYTE_SEQ_FROM_BYTES_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(expected.date, recordInDb.get("DATE_VALUE"))
    assertEquals(expected.dateOpt, Option(recordInDb.get("DATE_OPT")))
    assertEquals(expected.double, recordInDb.get("DOUBLE_VALUE").asInstanceOf[java.math.BigDecimal].doubleValue())
    assertEquals(expected.doubleOpt, Option(recordInDb.get("DOUBLE_OPT")).map(_.asInstanceOf[java.math.BigDecimal].doubleValue()))
    assertEquals(expected.float, recordInDb.get("FLOAT_VALUE").asInstanceOf[java.math.BigDecimal].floatValue())
    assertEquals(expected.floatOpt, Option(recordInDb.get("FLOAT_OPT")).map(_.asInstanceOf[java.math.BigDecimal].floatValue()))
    assertEquals(expected.int, recordInDb.get("INT_VALUE").asInstanceOf[java.math.BigDecimal].intValue())
    assertEquals(expected.intOpt, Option(recordInDb.get("INT_OPT")).map(_.asInstanceOf[java.math.BigDecimal].intValue()))
    assertEquals(expected.long, recordInDb.get("LONG_VALUE").asInstanceOf[java.math.BigDecimal].longValue())
    assertEquals(expected.longOpt, Option(recordInDb.get("LONG_OPT")).map(_.asInstanceOf[java.math.BigDecimal].longValue()))
    assertEquals(expected.nString, recordInDb.get("NSTRING"))
    assertEquals(expected.nStringOpt, Option(recordInDb.get("NSTRING_OPT")))
    assertEquals(expected.short, recordInDb.get("SHORT_VALUE").asInstanceOf[java.math.BigDecimal].shortValue())
    assertEquals(expected.shortOpt, Option(recordInDb.get("SHORT_OPT")).map(_.asInstanceOf[java.math.BigDecimal].shortValue()))
    assertEquals(expected.sqlXml, oracleXmlToTrimmedString(recordInDb.get("SQL_XML")))
    assertEquals(expected.sqlXmlOpt, Option(recordInDb.get("SQL_XML_OPT")).map(oracleXmlToTrimmedString))
    assertEquals(expected.stringFromAsciiStream, toUtf8String(recordInDb.get("STRING_FROM_ASCII_STREAM").asInstanceOf[Clob].getAsciiStream))
    assertEquals(expected.stringFromAsciiStreamOpt, Option(recordInDb.get("STRING_FROM_ASCII_STREAM_OPT")).map(r => toUtf8String(r.asInstanceOf[Clob].getAsciiStream)))
    assertEquals(expected.stringFromCharacterStream, readerToString(recordInDb.get("STRING_FROM_CHARACTER_STREAM").asInstanceOf[Clob].getCharacterStream))
    assertEquals(expected.stringFromCharacterStreamOpt, Option(recordInDb.get("STRING_FROM_CHARACTER_STREAM_OPT")).map(r => readerToString(r.asInstanceOf[Clob].getCharacterStream)))
    assertEquals(expected.stringFromClob, readerToString(recordInDb.get("STRING_FROM_CLOB").asInstanceOf[Clob].getCharacterStream))
    assertEquals(expected.stringFromClobOpt, Option(recordInDb.get("STRING_FROM_CLOB_OPT")).map(r => readerToString(r.asInstanceOf[Clob].getCharacterStream)))
    assertEquals(expected.stringFromNCharacterStream, recordInDb.get("STRING_FROM_NCHARACTER_STREAM"))
    assertEquals(expected.stringFromNCharacterStreamOpt, Option(recordInDb.get("STRING_FROM_NCHARACTER_STREAM_OPT")))
    assertEquals(expected.stringFromNClob, readerToString(recordInDb.get("STRING_FROM_NCLOB").asInstanceOf[NClob].getCharacterStream))
    assertEquals(expected.stringFromNClobOpt, Option(recordInDb.get("STRING_FROM_NCLOB_OPT")).map(r => readerToString(r.asInstanceOf[NClob].getCharacterStream)))
    assertEquals(expected.string, recordInDb.get("STRING"))
    assertEquals(expected.stringOpt, Option(recordInDb.get("STRING_OPT")))
    assertEquals(expected.time, recordInDb.get("TIME_VALUE"))
    assertEquals(expected.timeOpt, Option(recordInDb.get("TIME_OPT")))
    assertEquals(expected.timestamp, oracleTimestampToJavaTimestamp(recordInDb.get("TIMESTAMP_VALUE")))
    assertEquals(expected.timestampOpt, Option(recordInDb.get("TIMESTAMP_OPT")).map(oracleTimestampToJavaTimestamp))
    assertEquals(expected.url.toString, recordInDb.get("URL_VALUE"))
    assertEquals(expected.urlOpt.map(_.toString), Option(recordInDb.get("URL_OPT")))
  }

  override protected def recordToParams(record: OracleAllTypesRecord)(implicit conn: Connection) = {
    Seq[Any](
      record.id,
      new OracleSetArrayParam(Some(createOracleArray(conn, "INT_ARRAY_10", record.array.map(Int.box).toArray)), "INT_ARRAY_10") ,    //TODO: call `array.free()` after usage
      new OracleSetArrayParam(record.arrayOpt.map(a => createOracleArray(conn, "INT_ARRAY_10", a.map(Int.box).toArray)), "INT_ARRAY_10"),  //TODO: call `array.free()` after usage
      record.bigDecimal,
      record.bigDecimalOpt,
      record.boolean,
      record.booleanOpt,
      record.byte,
      record.byteOpt,
      record.byteSeqFromBinaryStream.toArray,
      record.byteSeqFromBinaryStreamOpt.map(_.toArray),
      record.byteSeqFromBlob.toArray,
      record.byteSeqFromBlobOpt.map(_.toArray),
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
      record.timestampOpt,
      record.url.toString,
      record.urlOpt.map(_.toString)
    )
  }

  private def oracleTimestampToJavaTimestamp(valueAsObject: AnyRef): Timestamp = {
    val oracleClass = Class.forName("oracle.sql.TIMESTAMP")
    val method = oracleClass.getMethod("timestampValue")
    method.invoke(oracleClass.cast(valueAsObject)).asInstanceOf[Timestamp]
  }

  private def oracleXmlToTrimmedString(valueAsObject: AnyRef): String = {
    val oracleClass = Class.forName("oracle.xdb.XMLType")
    val method = oracleClass.getMethod("getString")
    method.invoke(oracleClass.cast(valueAsObject)).asInstanceOf[String].trim
  }


  private def createOracleArray(conn:Connection, arrayTypeName: String, elements: AnyRef): Array = {
    val oracleClass = Class.forName("oracle.jdbc.driver.OracleConnection")
    val method = oracleClass.getMethod("createOracleArray", classOf[String], classOf[AnyRef])
    oracleClass.cast(conn)
    method.invoke(oracleClass.cast(conn), arrayTypeName, elements).asInstanceOf[Array]
  }

  private def sqlArrayToIntSeq(sqlArray: Array) = {
    sqlArrayToSeq[java.math.BigDecimal](sqlArray).map(_.intValue())
  }
}
