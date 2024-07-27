package org.sjr

import java.io.{BufferedReader, InputStream, Reader}
import java.sql
import java.sql.{Blob, Connection, SQLXML}
import scala.io.Source
import scala.util.Using

package object testutil {

  def toUtf8String(input: InputStream) = {
    Using.resource(Source.fromInputStream(input, "utf8")) {
      _.mkString
    }
  }

  def streamToByteSeq(input: InputStream): Seq[Byte] = {
    Using.resource(input) { input => LazyList.continually(input.read).takeWhile(_ != -1).map(_.toByte).toList }
  }


  def blobToByteSeq(blob: Blob): Seq[Byte] = {
    try {
      val input = blob.getBinaryStream
      LazyList.continually(input.read).takeWhile(_ != -1).map(_.toByte).toList
    } finally {
      blob.free()
    }

  }

  def readerToString(reader: Reader): String = {
    Using.resource(new BufferedReader(reader)) { br => LazyList.continually(br.readLine()).takeWhile(_ != null).mkString }
  }

  def toSQLXML(string: String)(implicit conn: Connection): SQLXML = {
    val xml = conn.createSQLXML()
    xml.setString(string)
    xml
  }


  def sqlArrayToSeq[T](sqlArray: sql.Array): Seq[T] = {
    try {
      sqlArray.getArray.asInstanceOf[Array[T]].toSeq
    } finally {
      sqlArray.free()
    }
  }
}
