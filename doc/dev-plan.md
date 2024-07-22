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
* Refactor - change names and re-org test cases
* Add quality control
    * separate test cases
    * test coverage --50%
    * code style -- don't optimize import as `._`
* Make it output libraries for multiple scala versions
* Publish the snapshot version
* Delete this file and publish the real version
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