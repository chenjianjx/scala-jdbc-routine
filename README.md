# scala-jdbc-routine

`scala-jdbc-routine` is a collection of helper classes designed to simplify JDBC programming in Scala.

It's similar to [Spring JdbcTemplate](https://spring.io/guides/gs/relational-data-access)
or [commons-dbutils](https://commons.apache.org/proper/commons-dbutils/examples.html), but offers a Scala-friendly API.

For example, it can work with `Option`.

```scala
val records = jdbcRoutine.queryForSeq(
  "select * from users where id = ?",
  new RowHandler[User] {
    override def handle(resultSet: WrappedResultSet): User = {
      User(
        id = resultSet.getScalaLong("id"),
        optionalName = resultSet.getStringOpt("name") // Option[] instead of null
      )
    }
  },
  Some(1) //parameters of Option[] are handled
)
```

## Features

* Plain JDBC
* Scala-friendly
* Tested with popular databases:  MySQL, Postgresql, Oracle and Microsoft SQL Server

## Quick Start

For `scala2.13`

```scala 
"com.github.chenjianjx.sjr" %% "scala-jdbc-routine" % "0.9.1"
```

For `scala3`

```scala
"com.github.chenjianjx.sjr" % "scala-jdbc-routine" % "0.9.1" cross CrossVersion.for3Use2_13
```

```scala
implicit val connection: Connection = howeverYouGetYourConnection()
val jdbcRoutine = new JdbcRoutine

jdbcRoutine.execute("CREATE TABLE users (id LONG PRIMARY KEY, name VARCHAR(255))")

jdbcRoutine.update("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Alice")
jdbcRoutine.update("INSERT INTO users (id, name) VALUES (?, ?)", 2, None)

val users = jdbcRoutine.queryForSeq("select * from users", new RowHandler[User] {
  override def handle(resultSet: WrappedResultSet): User = {
    User(id = resultSet.getScalaLong("id"),
      optioalName = resultSet.getStringOpt("name"))
  }
})

val aliceOpt = jdbcRoutine.queryForSingle("select * from users", new RowHandler[User] {
  override def handle(resultSet: WrappedResultSet): User = {
    User(id = resultSet.getScalaLong("id"),
      optioalName = resultSet.getStringOpt("name"))
  }
})

```

# Advanced usage

## Batching

```scala
jdbcRoutine.batchUpdate("INSERT INTO users (id, name) VALUES (?, ?)", 
  Seq(3, "Chris"), 
  Seq(4, "Dave")
)
```

## Get generated keys

### Auto-increment key

```scala
private class MySqlGeneratedKeysHandler extends GeneratedKeysHandler[Long] {
  override def handle(resultSet: WrappedResultSet): Option[Long] = if (resultSet.next()) {
    Some(resultSet.getScalaLong(1))
  } else {
    None
  }
}

val key = jdbcRoutine.updateAndGetGeneratedKeys[Long](
  "insert into some_table(int_value) values(?)",
  generatedKeysHandler,
  123
)
```

### Key from a sequence

```scala
private class OracleGeneratedKeysHandler extends GeneratedKeysHandler[Long] {
  override def handle(resultSet: WrappedResultSet): Option[Long] =
    if (resultSet.next()) {
      Some(resultSet.getScalaLong(1))
    } else {
      None
    }
}

val key = jdbcRoutine.updateAndGetGeneratedKeysFromReturnedColumns[Long](
  "insert into some_table(id, int_value) values(auto_key_seq.NEXTVAL, ?)",
  Array("id"),
  new OracleGeneratedKeysHandler, 123
)

```

## Stored Procedure / Function

```scala
jdbcRoutine.callToUpdate("CALL insert_sp (?, ?, ?, ?, ?, ?)",
  InParam(1.0F),
  InParam(Some(2.0F)),
  InOutParam(Types.DECIMAL, 100.0F),
  InOutParam(Types.DECIMAL, Some(200.0F)),
  OutParam(Types.FLOAT),
  OutParam(Types.FLOAT)
)
```

```scala
val records = jdbcRoutine.callForSeq("CALL query_sp ()", someRowHandler)
```

## Use `PreparedStatement.setXxx(...)`

By default, the library calls `preparedStatement.setObject(...)` to set plain scala values into a statement. Sometimes
this won't work and your jdbc driver requires `stmt.setXxx(...)` .

In this case, you need a `PreparedStatementSetterParam`

e.g.

```scala
class PgSetBytesAsBlobParam(bytes: Array[Byte]) extends PreparedStatementSetterParam {
  override def doSet(stmt: PreparedStatement, index: Int): Unit = {
    stmt.setBlob(index, new ByteArrayInputStream(bytes), bytes.length.toLong)
  }
}

jdbcRoutine.update(insertSql, someId, new PgSetBytesAsBlobParam(someBytes))
```

# Other features

## Transaction routine

```scala
implicit val connection: Connection = howeverYouGetYourConnection()
transaction {
  jdbcRoutine.update(insertSql, foo1, bar1)
  jdbcRoutine.update(insertSql, foo2, bar2)
}
```

See function [transaction](lib/src/main/scala/org/sjr/TransactionRoutine.scala)

## Scala-friendly ResultSet for vanilla JDBC

```scala
val vanillaResultSet = stmt.executeQuery()
val resultSet = new WrappedResultSet(vanillaResultSet)
while (resultSet.next()) {
  println(resultSet.getScalaLong("id"))
  println(resultSet.getStringOpt("name"))
}
```

See class [WrappedResultSet](lib/src/main/scala/org/sjr/WrappedResultSet.scala)
