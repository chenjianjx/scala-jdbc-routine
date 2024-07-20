package org.sjr

trait RowHandler[T] {
  def handle(resultSet: WrappedResultSet): T
}
