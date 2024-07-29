# Steps

* Generate the code skeleton -- done
* Write the API and its unit test + IT test -- can
  copy https://github.com/spring-attic/spring-scala/blob/master/src/main/scala/org/springframework/scala/jdbc/core/JdbcTemplate.scala
    * execute -- done
    * plain insert -- done
    * queryForSeq -- done
    * queryForSingle -- done
    * insert with returned key -- done
    * update data -- done
    * delete -- done
    * batchUpdate -- done
    * executeCallable -- done
    * support Blob & Clob and other getXxx() methods -- done
* Add transaction support -- done
* Refactor - change names and re-org test cases -- done
* Add quality control
    * code style -- don't optimize import as `._` -- done
    * add more scala compile options -- done
* vendor test cases
    * mysql --done
    * pg --done
    * oracle --done (with some types unsupported)
    * ms-sql -- done (with some types unsupported)
* Things to revise
    * Instead of stringAsXxxStream in the crud test case class, use File to simulate real life?  -- no thanks
    * blob/xml/etc for some databases -- done
* Make it output libraries for multiple scala versions -- give up cross-build . Just use 2.13
* Publish the snapshot version
  * To sonatype -- done 
* Publish the real version
* Write readme and delete this file
* Promotion

----

# Design

## for callable statements

* Both input parameters and output parameters will have a `?` in the sql
    * The input params will be set using stmt.setXXX(value)
    * The output params will be set using stmt.registerOutputParameter(sqlType)
    * The inout params will be set using both
* After execution
    * The stored procedure itself may return a result set or multiple result sets
    * The output params and inout params will be set

### First step - assuming only one result set is returned

* Create a data structure called Parameter for user to pick parameter type -- sorry they can't pass in pojo directly
* A row handler will be enough to handle returned result set
* The returned result will be a tuple of (result parsed from result set, MAP[paramIndex, value of Output params] )

## To support all strange types

The standard jdbc API let you set blob/clob/nclob/binarystream/asiccstream/charaterstream/ncharacterstream in the
following ways:

From `PreparedStatement`

*     void setBlob (int parameterIndex, Blob x) throws SQLException;
*     void setBlob(int parameterIndex, InputStream inputStream) throws SQLException;
*     void setBlob(int parameterIndex, InputStream inputStream, int length) throws SQLException;

From `Connection`:  only supports clob/nclob and blob

* createClob

-----

Support in vendors:

| vendor    | stmt.setAsciiStream | stmt.setCharacterStream | stmt.setNCaracterStream | stmt.setBinaryStream | stmt.setBlob | stmt.setClob  | stmt.setNClob | stmt.setSQLXML | conn.createBlob | conn.createNClob | conn.createBlob | conn.createSQLXML |
|-----------|---------------------|-------------------------|-------------------------|----------------------|--------------|---------------|---------------|----------------|-----------------|------------------|-----------------|-------------------|
| pg        | N                   | non-length set          | N                       | All                  | All          | setClob(clob) | N             | Y              | N               | N                | N               | Y                 |
| sqlserver | All                 | All                     | All                     | All                  | All          | All           | All           | Y              | Y               | Y                | Y               | Y                 |
| oracle    | All                 | All                     | All                     | All                  | All          | All           | All           | Y              | Y               | Y                | Y               | Y                 |
| mysql     | All                 | All                     | All                     | All                  | All          | All           | All           | Y              | Y               | Y                | Y               | Y                 |
|           |                     |                         |                         |                      |              |               |               |                |                 |                  |                 |                   |
|           |                     |                         |                         |                      |              |               |               |                |                 |                  |                 |                   |

### So how should they be supported ? 

* option 1 -> use connection to create a param and pass in the parameter --> not supported well by PG, and setting values to created param can be tedious
* option 2 -> allow a StatementSetter to be a parameter, where a method can be implemented:  set[T](index, T) , T is a plain type, not a strange type  -- let's just do this