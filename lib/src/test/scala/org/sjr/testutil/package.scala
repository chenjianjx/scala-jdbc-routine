package org.sjr

import java.io.{BufferedReader, InputStream, Reader}
import scala.io.Source
import scala.util.Using

package object testutil {

  def toUtf8String(input: InputStream) = {
    Using.resource(Source.fromInputStream(input, "utf8")) {
      _.mkString
    }
  }

  def toByteSeq(input: InputStream): Seq[Byte] = {
    Using.resource(input) { input => LazyList.continually(input.read).takeWhile(_ != -1).map(_.toByte) }
  }

  def readerToString(reader: Reader): String = {
    Using.resource(new BufferedReader(reader)) { br => LazyList.continually(br.readLine()).takeWhile(_ != null).mkString }
  }

}
