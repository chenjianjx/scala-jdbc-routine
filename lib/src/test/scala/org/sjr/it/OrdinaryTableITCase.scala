package org.sjr.it

import org.apache.commons.dbutils.handlers.MapListHandler
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test}
import org.sjr.TransactionRoutine.transaction
import org.sjr.it.OrdinaryTableITCase.{AllFieldsRecord, InsertSql, RequiredFieldsRecord}
import org.sjr.testutil.{readerToString, toByteSeq, toUtf8String}
import org.sjr.{JdbcRoutine, RowHandler, WrappedResultSet}
import org.testcontainers.containers.MySQLContainer

import java.net.URL
import java.sql.{SQLException, SQLNonTransientException}
import java.util
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._


case class AllTypesRecord(
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
                           timestampOpt: Option[java.sql.Timestamp],
                           url: URL,
                           urlOpt: Option[URL]
                         )

object OrdinaryTableITCase {
  private implicit val testContainer: MySQLContainer[Nothing]  = createTestContainer()

  private val TableDdl =
    """
      |CREATE TABLE all_types_record (
      |  id BIGINT PRIMARY KEY,
      |
      |  big_decimal DECIMAL(10, 2) NOT NULL,
      |  big_decimal_opt DECIMAL(10, 2),
      |  boolean_value BOOLEAN NOT NULL,
      |  boolean_opt BOOLEAN,
      |  byte_value TINYINT NOT NULL,
      |  byte_opt TINYINT,
      |  byte_seq_from_binary_stream VARBINARY(100) NOT NULL,
      |  byte_seq_from_binary_stream_opt VARBINARY(100),
      |  byte_seq_from_blob BLOB NOT NULL,
      |  byte_seq_from_blob_opt BLOB,
      |  byte_seq_from_bytes VARBINARY(100) NOT NULL,
      |  byte_seq_from_bytes_opt VARBINARY(100),
      |  date_value DATE NOT NULL,
      |  date_opt DATE,
      |  double_value DOUBLE NOT NULL,
      |  double_opt DOUBLE,
      |  float_value FLOAT NOT NULL,
      |  float_opt FLOAT,
      |  int_value INT NOT NULL,
      |  int_opt INT,
      |  long_value BIGINT NOT NULL,
      |  long_opt BIGINT,
      |  nstring VARCHAR(2) NOT NULL,
      |  nstring_opt VARCHAR(2),
      |  short_value SMALLINT NOT NULL,
      |  short_opt SMALLINT,
      |  sql_xml LONGTEXT NOT NULL,
      |  sql_xml_opt LONGTEXT,
      |  string_from_ascii_stream LONGTEXT NOT NULL,
      |  string_from_ascii_stream_opt LONGTEXT,
      |  string_from_character_stream LONGTEXT NOT NULL,
      |  string_from_character_stream_opt LONGTEXT,
      |  string_from_clob LONGTEXT NOT NULL,
      |  string_from_clob_opt LONGTEXT,
      |  string_from_ncharacter_stream NVARCHAR(2) NOT NULL,
      |  string_from_ncharacter_stream_opt NVARCHAR(2),
      |  string_from_nclob LONGTEXT NOT NULL,
      |  string_from_nclob_opt LONGTEXT,
      |  string VARCHAR(20) NOT NULL,
      |  string_opt VARCHAR(20),
      |  time_value TIME NOT NULL,
      |  time_opt TIME,
      |  timestamp_value TIMESTAMP NOT NULL,
      |  timestamp_opt TIMESTAMP,
      |  url_value VARCHAR(2083) NOT NULL,
      |  url_opt VARCHAR(2083)
      |)
      |""".stripMargin


  private val InsertSql =
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
      |    timestamp_opt,
      |    url_value,
      |    url_opt
      |) VALUES (
      |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      |);
      |""".stripMargin


  private val AllFieldsRecord = AllTypesRecord(
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
    timestampOpt = Some(java.sql.Timestamp.valueOf("2024-02-02 01:23:45")),
    url = new URL("https://www.foo.com"),
    urlOpt = Some(new URL("http://localhost:8080"))
  )

  private val RequiredFieldsRecord = AllTypesRecord(
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
    timestampOpt = None,
    url = new URL("https://www.foo.com"),
    urlOpt = None

  )


  @BeforeAll
  def beforeAll(): Unit = {
    testContainer.start()

    withConn { conn =>
      testJdbc.execute(conn, TableDdl)
      ()
    }
  }

  @AfterAll
  def afterAll(): Unit = {
    testContainer.close()
  }

}


class OrdinaryTableITCase {

  import OrdinaryTableITCase.testContainer

  val jdbcRoutine = new JdbcRoutine

  @BeforeEach
  def beforeEach(): Unit = {
    withConn { conn =>
      testJdbc.update(conn, "DELETE FROM all_types_record")
      ()
    }
  }

  @Test
  def insert(): Unit = {
    withConn { conn =>
      assertEquals(1, jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)(conn))
      assertEquals(1, jdbcRoutine.update(InsertSql, recordToParams(RequiredFieldsRecord): _*)(conn))
      val recordsInDb = testJdbc.query(conn, s"select * from all_types_record order by id", new MapListHandler()).asScala.toSeq
      assertEquals(2, recordsInDb.size)
      assertSameData(AllFieldsRecord, recordsInDb(0))
      assertSameData(RequiredFieldsRecord, recordsInDb(1))
    }
  }

  @Test
  def queryForSeq(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)
      jdbcRoutine.update(InsertSql, recordToParams(RequiredFieldsRecord): _*)

      val rows = jdbcRoutine.queryForSeq("SELECT * FROM all_types_record where id in (?, ?, ?) ORDER BY id", new SomeRecordRowHandler(), -1L, RequiredFieldsRecord.id, AllFieldsRecord.id)
      assertEquals(2, rows.size)
      assertEquals(AllFieldsRecord, rows(0))
      assertEquals(RequiredFieldsRecord, rows(1))
    }
  }

  @Test
  def queryForSeq_byIndexRowHandler(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)
      jdbcRoutine.update(InsertSql, recordToParams(RequiredFieldsRecord): _*)

      val rows = jdbcRoutine.queryForSeq("SELECT * FROM all_types_record where id in (?, ?, ?) ORDER BY id", new SomeRecordByIndexRowHandler(), -1L, RequiredFieldsRecord.id, AllFieldsRecord.id)
      assertEquals(2, rows.size)
      assertEquals(AllFieldsRecord, rows(0))
      assertEquals(RequiredFieldsRecord, rows(1))
    }
  }


  @Test
  def queryForSingleRow(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)
      val rows = jdbcRoutine.queryForSeq("SELECT * FROM all_types_record where id = ?", new SomeRecordRowHandler(), AllFieldsRecord.id)
      assertEquals(1, rows.size)
      assertEquals(AllFieldsRecord, rows(0))
    }
  }

  @Test
  def queryForSingleRowSingleColumn(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)

      assertEquals(Some(AllFieldsRecord.string), jdbcRoutine.queryForSingle("SELECT * FROM all_types_record where id = ?", _.getString("STRING"), AllFieldsRecord.id))
      assertEquals(AllFieldsRecord.stringOpt, jdbcRoutine.queryForSingle("SELECT * FROM all_types_record where id = ?", _.getStringOpt("STRING_OPT"), AllFieldsRecord.id).flatten)

      assertEquals(None, jdbcRoutine.queryForSingle("SELECT * FROM all_types_record where id = ?", _.getStringOpt("STRING"), -1))
      assertEquals(None, jdbcRoutine.queryForSingle("SELECT * FROM all_types_record where id = ?", _.getStringOpt("STRING_OPT"), -1))
    }
  }

  @Test
  def query_wantValuePresentButNull(): Unit = {

    withConn { implicit conn =>
      jdbcRoutine.update(InsertSql, recordToParams(RequiredFieldsRecord): _*)

      val exception = assertThrows(classOf[SQLNonTransientException], () => jdbcRoutine.queryForSeq(s"select * from all_types_record where id = ?", _.getInt("INT_OPT"), RequiredFieldsRecord.id).asInstanceOf[Unit])
      assertTrue(exception.getMessage.contains("INT_OPT"))

    }
  }

  @Test
  def updateRows(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)
      jdbcRoutine.update(InsertSql, recordToParams(RequiredFieldsRecord): _*)

      assertEquals(1, jdbcRoutine.update("UPDATE all_types_record SET int_value = ? where id = ? ", 999, AllFieldsRecord.id))

      val rows = jdbcRoutine.queryForSeq("SELECT * FROM all_types_record ORDER BY id", new SomeRecordRowHandler())
      assertEquals(AllFieldsRecord.copy(int = 999), rows(0))
      assertEquals(RequiredFieldsRecord, rows(1))
    }
  }


  @Test
  def delete(): Unit = {
    withConn { implicit conn =>
      jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)
      jdbcRoutine.update(InsertSql, recordToParams(RequiredFieldsRecord): _*)

      jdbcRoutine.update("DELETE FROM all_types_record where id = ?", AllFieldsRecord.id)
      val rows = jdbcRoutine.queryForSeq("SELECT * FROM all_types_record ORDER BY id", new SomeRecordRowHandler())
      assertEquals(1, rows.size)
      assertEquals(RequiredFieldsRecord, rows(0))
    }
  }

  @Test
  def batchInsert(): Unit = {
    withConn { implicit conn =>
      assertArrayEquals(scala.Array(1, 1), jdbcRoutine.batchUpdate(InsertSql, recordToParams(AllFieldsRecord), recordToParams(RequiredFieldsRecord)))

      val rows = jdbcRoutine.queryForSeq("SELECT * FROM all_types_record where id in (?, ?, ?) ORDER BY id", new SomeRecordRowHandler(), -1L, RequiredFieldsRecord.id, AllFieldsRecord.id)
      assertEquals(2, rows.size)
      assertEquals(AllFieldsRecord, rows(0))
      assertEquals(RequiredFieldsRecord, rows(1))
    }
  }


  @Test
  def insertRowsInTransaction_allGood(): Unit = {
    withConn { implicit conn =>
      transaction {
        jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)
        jdbcRoutine.update(InsertSql, recordToParams(RequiredFieldsRecord): _*)
      }
    }

    withConn { implicit conn =>
      val idSeq = jdbcRoutine.queryForSeq("SELECT id FROM all_types_record ORDER BY id", _.getLong("id"))
      assertEquals(Seq(AllFieldsRecord.id, RequiredFieldsRecord.id), idSeq)
    }
  }

  @Test
  def insertRowsInTransaction_withRollback(): Unit = {
    withConn { implicit conn =>
      try {
        transaction {
          jdbcRoutine.update(InsertSql, recordToParams(AllFieldsRecord): _*)
          jdbcRoutine.update(InsertSql, recordToParams(RequiredFieldsRecord.copy(string = "12345678901234567890_OVER_LONG")): _*)
        }
        fail("Shouldn't be here")
      } catch {
        case e: SQLException => println(e.getMessage)
      }
    }

    withConn { implicit conn =>
      val idSeq = jdbcRoutine.queryForSeq("SELECT id FROM all_types_record ORDER BY id", _.getLong("id"))
      assertEquals(Seq(), idSeq)
    }
  }


  private def recordToParams(record: AllTypesRecord) = {
    Seq[Any](
      record.id,

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


  private class SomeRecordRowHandler extends RowHandler[AllTypesRecord] {

    override def handle(rs: WrappedResultSet): AllTypesRecord = AllTypesRecord(
      id = rs.getLong("ID"),

      bigDecimal = rs.getBigDecimal("BIG_DECIMAL"),
      bigDecimalOpt = rs.getBigDecimalOpt("BIG_DECIMAL_OPT"),
      boolean = rs.getBoolean("BOOLEAN_VALUE"),
      booleanOpt = rs.getBooleanOpt("BOOLEAN_OPT"),
      byte = rs.getByte("BYTE_VALUE"),
      byteOpt = rs.getByteOpt("BYTE_OPT"),
      byteSeqFromBinaryStream = toByteSeq(rs.getBinaryStream("BYTE_SEQ_FROM_BINARY_STREAM")),
      byteSeqFromBinaryStreamOpt = rs.getBinaryStreamOpt("BYTE_SEQ_FROM_BINARY_STREAM_OPT").map(toByteSeq),
      byteSeqFromBlob = toByteSeq(rs.getBlob("BYTE_SEQ_FROM_BLOB").getBinaryStream),
      byteSeqFromBlobOpt = rs.getBlobOpt("BYTE_SEQ_FROM_BLOB_OPT").map(blob => toByteSeq(blob.getBinaryStream)),
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
      nString = rs.getNString("NSTRING"),
      nStringOpt = rs.getNStringOpt("NSTRING_OPT"),
      short = rs.getShort("SHORT_VALUE"),
      shortOpt = rs.getShortOpt("SHORT_OPT"),
      sqlXml = rs.getSQLXML("SQL_XML").getString,
      sqlXmlOpt = rs.getSQLXMLOpt("SQL_XML_OPT").flatMap(sx => Option(sx.getString)), //mysql jdbc driver problem: null column value won't return NULL. TODO: submit a bug to them
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


  private class SomeRecordByIndexRowHandler extends RowHandler[AllTypesRecord] {

    override def handle(rs: WrappedResultSet): AllTypesRecord = {
      val i = new AtomicInteger(1)
      AllTypesRecord(
        id = rs.getLong(i.getAndIncrement),

        bigDecimal = rs.getBigDecimal(i.getAndIncrement),
        bigDecimalOpt = rs.getBigDecimalOpt(i.getAndIncrement),
        boolean = rs.getBoolean(i.getAndIncrement),
        booleanOpt = rs.getBooleanOpt(i.getAndIncrement),
        byte = rs.getByte(i.getAndIncrement),
        byteOpt = rs.getByteOpt(i.getAndIncrement),
        byteSeqFromBinaryStream = toByteSeq(rs.getBinaryStream(i.getAndIncrement)),
        byteSeqFromBinaryStreamOpt = rs.getBinaryStreamOpt(i.getAndIncrement).map(toByteSeq),
        byteSeqFromBlob = toByteSeq(rs.getBlob(i.getAndIncrement).getBinaryStream),
        byteSeqFromBlobOpt = rs.getBlobOpt(i.getAndIncrement).map(blob => toByteSeq(blob.getBinaryStream)),
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
        nString = rs.getNString(i.getAndIncrement),
        nStringOpt = rs.getNStringOpt(i.getAndIncrement),
        short = rs.getShort(i.getAndIncrement),
        shortOpt = rs.getShortOpt(i.getAndIncrement),
        sqlXml = rs.getSQLXML(i.getAndIncrement).getString,
        sqlXmlOpt = rs.getSQLXMLOpt(i.getAndIncrement).flatMap(sx => Option(sx.getString)), //mysql jdbc driver problem: null column value won't return NULL. TODO: submit a bug to them
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

  private def assertSameData(predefinedRecord: AllTypesRecord, recordFromDb: util.Map[String, Object]): Unit = {
    assertEquals(predefinedRecord.id, recordFromDb.get("ID"))

    assertEquals(predefinedRecord.bigDecimal.bigDecimal, recordFromDb.get("BIG_DECIMAL"))
    assertEquals(predefinedRecord.bigDecimalOpt.map(_.bigDecimal), Option(recordFromDb.get("BIG_DECIMAL_OPT")))
    assertEquals(predefinedRecord.boolean, recordFromDb.get("BOOLEAN_VALUE"))
    assertEquals(predefinedRecord.booleanOpt, Option(recordFromDb.get("BOOLEAN_OPT")))
    assertEquals(predefinedRecord.byte.toInt, recordFromDb.get("BYTE_VALUE"))
    assertEquals(predefinedRecord.byteOpt.map(_.toInt), Option(recordFromDb.get("BYTE_OPT")))
    assertEquals(predefinedRecord.byteSeqFromBinaryStream, recordFromDb.get("BYTE_SEQ_FROM_BINARY_STREAM").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(predefinedRecord.byteSeqFromBinaryStreamOpt, Option(recordFromDb.get("BYTE_SEQ_FROM_BINARY_STREAM_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(predefinedRecord.byteSeqFromBlob, recordFromDb.get("BYTE_SEQ_FROM_BLOB").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(predefinedRecord.byteSeqFromBlobOpt, Option(recordFromDb.get("BYTE_SEQ_FROM_BLOB_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(predefinedRecord.byteSeqFromBytes, recordFromDb.get("BYTE_SEQ_FROM_BYTES").asInstanceOf[scala.Array[Byte]].toSeq)
    assertEquals(predefinedRecord.byteSeqFromBytesOpt, Option(recordFromDb.get("BYTE_SEQ_FROM_BYTES_OPT")).map(_.asInstanceOf[scala.Array[Byte]].toSeq))
    assertEquals(predefinedRecord.date, recordFromDb.get("DATE_VALUE"))
    assertEquals(predefinedRecord.dateOpt, Option(recordFromDb.get("DATE_OPT")))
    assertEquals(predefinedRecord.double, recordFromDb.get("DOUBLE_VALUE"))
    assertEquals(predefinedRecord.doubleOpt, Option(recordFromDb.get("DOUBLE_OPT")))
    assertEquals(predefinedRecord.float, recordFromDb.get("FLOAT_VALUE"))
    assertEquals(predefinedRecord.floatOpt, Option(recordFromDb.get("FLOAT_OPT")))
    assertEquals(predefinedRecord.int, recordFromDb.get("INT_VALUE"))
    assertEquals(predefinedRecord.intOpt, Option(recordFromDb.get("INT_OPT")))
    assertEquals(predefinedRecord.long, recordFromDb.get("LONG_VALUE"))
    assertEquals(predefinedRecord.longOpt, Option(recordFromDb.get("LONG_OPT")))
    assertEquals(predefinedRecord.nString, recordFromDb.get("NSTRING"))
    assertEquals(predefinedRecord.nStringOpt, Option(recordFromDb.get("NSTRING_OPT")))
    assertEquals(predefinedRecord.short.toInt, recordFromDb.get("SHORT_VALUE"))
    assertEquals(predefinedRecord.shortOpt.map(_.toInt), Option(recordFromDb.get("SHORT_OPT")))
    assertEquals(predefinedRecord.sqlXml, recordFromDb.get("SQL_XML"))
    assertEquals(predefinedRecord.sqlXmlOpt, Option(recordFromDb.get("SQL_XML_OPT")))
    assertEquals(predefinedRecord.stringFromAsciiStream, recordFromDb.get("STRING_FROM_ASCII_STREAM"))
    assertEquals(predefinedRecord.stringFromAsciiStreamOpt, Option(recordFromDb.get("STRING_FROM_ASCII_STREAM_OPT")))
    assertEquals(predefinedRecord.stringFromCharacterStream, recordFromDb.get("STRING_FROM_CHARACTER_STREAM"))
    assertEquals(predefinedRecord.stringFromCharacterStreamOpt, Option(recordFromDb.get("STRING_FROM_CHARACTER_STREAM_OPT")))
    assertEquals(predefinedRecord.stringFromClob, recordFromDb.get("STRING_FROM_CLOB"))
    assertEquals(predefinedRecord.stringFromClobOpt, Option(recordFromDb.get("STRING_FROM_CLOB_OPT")))
    assertEquals(predefinedRecord.stringFromNCharacterStream, recordFromDb.get("STRING_FROM_NCHARACTER_STREAM"))
    assertEquals(predefinedRecord.stringFromNCharacterStreamOpt, Option(recordFromDb.get("STRING_FROM_NCHARACTER_STREAM_OPT")))
    assertEquals(predefinedRecord.stringFromNClob, recordFromDb.get("STRING_FROM_NCLOB"))
    assertEquals(predefinedRecord.stringFromNClobOpt, Option(recordFromDb.get("STRING_FROM_NCLOB_OPT")))
    assertEquals(predefinedRecord.string, recordFromDb.get("STRING"))
    assertEquals(predefinedRecord.stringOpt, Option(recordFromDb.get("STRING_OPT")))
    assertEquals(predefinedRecord.time, recordFromDb.get("TIME_VALUE"))
    assertEquals(predefinedRecord.timeOpt, Option(recordFromDb.get("TIME_OPT")))
    assertEquals(predefinedRecord.timestamp, recordFromDb.get("TIMESTAMP_VALUE"))
    assertEquals(predefinedRecord.timestampOpt, Option(recordFromDb.get("TIMESTAMP_OPT")))
    assertEquals(predefinedRecord.url.toString, recordFromDb.get("URL_VALUE"))
    assertEquals(predefinedRecord.urlOpt.map(_.toString), Option(recordFromDb.get("URL_OPT")))
  }

}
