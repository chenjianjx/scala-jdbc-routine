package org.sjr

import org.sjr.callable.{CallForDataResult, CallToUpdateResult, CallableStatementParam, InOutParam, InParam, OutParam}

import java.sql.{CallableStatement, Connection, PreparedStatement, ResultSet, SQLException, Statement}
import scala.collection.mutable.ArrayBuffer
import scala.util.Using

class JdbcRoutine {

  /**
   * typically used for DDL
   */
  @throws[SQLException]
  def execute(sql: String)(implicit conn: Connection): Unit = {
    Using.resource(conn.createStatement()) { stmt =>
      stmt.execute(sql).asInstanceOf[Unit]
    }
  }

  /**
   * @return the return result of underlying [[java.sql.Statement#executeUpdate(String)]]
   */
  @throws[SQLException]
  def update(sql: String, params: Any*)(implicit conn: Connection): Int = {
    withPreparedStatement(sql, params) { stmt =>
      stmt.executeUpdate();
    }
  }

  /**
   * @return ( the return result of underlying [[java.sql.Statement#executeUpdate(String)]] , the generated keys)
   */
  @throws[SQLException]
  def updateAndGetGeneratedKeys[KEYS](sql: String, generatedKeysHandler: GeneratedKeysHandler[KEYS], params: Any*)(implicit conn: Connection): (Int, Option[KEYS]) = {
    Using.resource(conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) { stmt =>
      setParams(stmt, params)
      val execResult = stmt.executeUpdate()
      val resultSet = new WrappedResultSet(stmt.getGeneratedKeys)
      val generatedKeys = generatedKeysHandler.handle(resultSet)
      (execResult, generatedKeys)
    }
  }

  /**
   *
   * @param returnedColumnNames an array of column names indicating the columns that should be returned from the inserted row or rows. Useful in Sequence-based auto key generation such as Oracle
   * @return ( the return result of underlying [[java.sql.Statement#executeUpdate(String)]] , the generated keys)
   */
  @throws[SQLException]
  def updateAndGetGeneratedKeysFromReturnedColumns[KEYS](sql: String, returnedColumnNames: Array[String], generatedKeysHandler: GeneratedKeysHandler[KEYS], params: Any*)(implicit conn: Connection): (Int, Option[KEYS]) = {
    Using.resource(conn.prepareStatement(sql, returnedColumnNames)) { stmt =>
      setParams(stmt, params)
      val execResult = stmt.executeUpdate()
      val resultSet = new WrappedResultSet(stmt.getGeneratedKeys)
      val generatedKeys = generatedKeysHandler.handle(resultSet)
      (execResult, generatedKeys)
    }
  }


  @throws[SQLException]
  def queryForSeq[T](sql: String, rowHandler: RowHandler[T], params: Any*)(implicit conn: Connection): Seq[T] = {
    withPreparedStatement(sql, params) { stmt =>
      resultSetToRecords(stmt.executeQuery(), rowHandler)
    }
  }

  @throws[SQLException]
  def queryForSingle[T](sql: String, rowHandler: RowHandler[T], params: Any*)(implicit conn: Connection): Option[T] = {
    queryForSeq(sql, rowHandler, params: _*).headOption
  }

  /**
   *
   * @return the return result of underlying [[java.sql.Statement#executeBatch()]]
   */
  @throws[SQLException]
  def batchUpdate(sql: String, paramRows: Seq[Any]*)(implicit conn: Connection): scala.Array[Int] = {
    Using.resource(conn.prepareStatement(sql)) { stmt =>
      for (paramRow <- paramRows) {
        setParams(stmt, paramRow)
        stmt.addBatch()
      }
      stmt.executeBatch()
    }
  }

  /**
   * Invoke a stored procedure/function and retrieve the result, as well as values in out parameters
   * NOTE: If your stored procedure/function returns multiple ResultSet, only the first ResultSset is handled
   *
   * @param rowHandler Note: only for the first ResultSet returned
   */
  @throws[SQLException]
  def callForSeq[T](sql: String, rowHandler: RowHandler[T], params: CallableStatementParam*)(implicit conn: Connection): CallForDataResult[T] = {
    Using.resource(conn.prepareCall(sql)) { stmt =>
      setParamsForCall(params, stmt)

      val hasResultSet = stmt.execute()

      val records = if (hasResultSet) {
        resultSetToRecords(stmt.getResultSet(), rowHandler)
      } else {
        if (stmt.getMoreResults) { //When there are result sets, some jdbc drivers (e.g. Oracle) return false for `stmt.execute()` but true for `getMoreResults()`
          resultSetToRecords(stmt.getResultSet(), rowHandler)
        } else {
          Seq()
        }
      }

      val outValues = getOutValuesAfterCall(stmt, params)

      CallForDataResult(records, outValues)
    }
  }

  /**
   * Invoke a stored procedure/function, as well as values in out parameters
   */
  @throws[SQLException]
  def callToUpdate(sql: String, params: CallableStatementParam*)(implicit conn: Connection): CallToUpdateResult = {
    Using.resource(conn.prepareCall(sql)) { stmt =>
      setParamsForCall(params, stmt)
      stmt.execute()
      val outValues = getOutValuesAfterCall(stmt, params)
      CallToUpdateResult(outValues)
    }
  }

  private def resultSetToRecords[T](vanillaResultSet: ResultSet, rowHandler: RowHandler[T]) = {
    val resultSet = new WrappedResultSet(vanillaResultSet)
    val records = new ArrayBuffer[T]
    while (resultSet.next()) {
      records += rowHandler.handle(resultSet)
    }
    records.toSeq
  }

  private def withPreparedStatement[T](sql: String, params: Seq[Any])(job: PreparedStatement => T)(implicit conn: Connection): T = {
    Using.resource(conn.prepareStatement(sql)) { stmt =>
      setParams(stmt, params)
      job(stmt)
    }
  }

  private def setParams(stmt: PreparedStatement, params: Seq[Any]): Unit = {
    for (paramIndex <- 0 to params.length - 1) {
      stmt.setObject(paramIndex + 1, toJavaJdbcValue(params(paramIndex)))
    }
  }

  private def setParamsForCall(params: Seq[CallableStatementParam], stmt: CallableStatement): Unit = {
    for (paramIndex <- 0 to params.length - 1) {
      val param = params(paramIndex)
      param match {
        case InParam(value) => stmt.setObject(paramIndex + 1, toJavaJdbcValue(value))
        case OutParam(sqlType) => stmt.registerOutParameter(paramIndex + 1, sqlType)
        case InOutParam(sqlType, value) =>
          stmt.registerOutParameter(paramIndex + 1, sqlType)
          stmt.setObject(paramIndex + 1, toJavaJdbcValue(value), sqlType)
      }
    }
  }

  private def getOutValuesAfterCall[T](stmt: CallableStatement, params: Seq[CallableStatementParam]) = {
    params.zipWithIndex.filter { case (param, _) => param.canOutput }
      .map { case (_, index) => (index + 1) -> stmt.getObject(index + 1) }
      .filter { case (_, v) => v != None.orNull }
      .map { case (k, v) => k -> nonNullJdbcValueToScalaValue(v) }
      .toMap
  }


  private def toJavaJdbcValue(scalaValue: Any) = {
    scalaValue match {
      case Some(v: BigDecimal) => v.bigDecimal
      case Some(v) => v
      case None => None.orNull
      case v: BigDecimal => v.bigDecimal
      case _ => scalaValue
    }
  }

  private def nonNullJdbcValueToScalaValue(jdbcValue: Any) = {
    jdbcValue match {
      case v: java.math.BigDecimal => BigDecimal(v)
      case _ => jdbcValue
    }
  }
}
