Prequel - SQL is enough (A little revisited code and capabilities)
==================================================================

There are a lot of database libraries out there. Most of them try to create a new abstraction on top of SQL. I think SQL is already a quite nice abstraction for working with data. Prequel aims to make working with this abstraction a bit more comfortable, nothing more.

[![Build Status](https://secure.travis-ci.org/vannicaruso/prequel.png)](http://travis-ci.org/vannicaruso/prequel)

### Background

Prequel is a small set of classes making handling of SQL queries in Scala a bit easier. It takes care of connection handling/pooling, sql escaping, parameter conversion and to some extent transaction handling.

Prequel was written by  [Johan Persson](https://github.com/jpersson) since he was not really happy with what I could find in terms of jdbc based database libraries. The library is heavily inspired by projects like [Querulous](https://github.com/nkallen/querulous), [Simplifying JDBC](http://scala.sygneca.com/code/simplifying-jdbc) and unreleased work of [Tristan Juricek](https://github.com/tristanjuricek).

See example below how prequel can make your life easier.

### Database Compatibility

Prequel should be compatible with most JDBC supported databases. I've only tested it using HSQLDB and PostgreSQL but MySQL and others should work fine. I [Giovanni Caruso](https://github.com/vannicaruso) tested it with oracle 11g v. 11.2.0.2 too.

### Use at your own risk

Although I'm using this library in my own projects I have not tested it with massive amounts of data so use at your own risk :-)

### Logging

Logging is enabled via slf4j api.

### Not supported

 * Any config files for database configuration
 * Any type of ORM voodoo (and will never be)

Examples
--------

Given the following import and definitions

```scala
import net.noerd.prequel.DatabaseConfig
import net.noerd.prequel.SQLFormatterImplicits._
import net.noerd.prequel.ResultSetRowImplicits._

case class Bicycle( id: Long, brand: String, releaseDate: DateTime )

val database = DatabaseConfig(
    driver = "org.hsqldb.jdbc.JDBCDriver",
    jdbcURL = "jdbc:hsqldb:mem:mymemdb"
)
```

Prequel makes it quite comfortable for you to do:

## Inserts

```scala
def insertBicycle( bike: Bicycle ): Unit = {
    database.transaction { tx => 
        tx.execute( 
            "insert into bicycles( id, brand, release_date ) values( ?, ?, ? )", 
            bike.id, bike.brand, bike.releaseDate
        )
    }
}
```
## Batch Updates and Inserts

```scala
def insertBicycles( bikes: Seq[ Bicycle ] ): Unit = {
    database.transaction { tx => 
      tx.executeBatch( "insert into bicycles( id, brand, release_date ) values( ?, ?, ? )" ) { statement => 
        bikes.foreach { bike =>
          statment.executeWith( bike.id, bike.brand, bike.releaseDate )
        }
      }
    }
}
```
 
## Easily create objects from selects

```scala
def fetchBicycles(): Seq[ Bicycles ] = {
    database.transaction { tx => 
        Database(tx.connection).select( "select id, brand, release_date from bicycles" ) { r =>
            Bicycle( r, r, r )
        }
    }
}
```

## Select native types directly

```scala
def fetchBicycleCount: Long = {
    database.transaction { tx => 
        Database(tx.connection).selectLong( "select count(*) from bicycles")
    }
}
```

## Use an external Connection

```scala
val conn = ...
Database(conn).select( "select id, brand, release_date from bicycles" ) { r =>
  Bicycle( r, r, r )
}
```

## Use a jndi Connection from some DataSource

```scala
Database("jdbc/[Something]").select( "select id, brand, release_date from bicycles" ) { r =>
  Bicycle( r, r, r )
}
```

Use in your Project
-------------------

This version is not published so to use in your projects you have to clone repository and build by yourself.


Dependencies
------------

### 3rd Party libs

I've tried to keep the list of dependencies as short as possible but currently the following
libraries are being used.

* [commons-pool 1.5.5](http://commons.apache.org/pool) for general object pooling
* [commons-dbcp 1.4](http://commons.apache.org/dbcp) for the more db specific parts of connection pools
* [commons-lang 2.6](http://commons.apache.org/lang) for SQL escaping
* [joda-time 2.2](http://joda-time.sourceforge.net/) for sane support of Date and Time
* [joda-convert 1.3.1]

### Testing

For testing I use [scala-test](http://www.scalatest.org) for unit-tests and [hsqldb](http://hsqldb.org) for in process db interaction during tests.

Feedback
--------

If you have any questions or feedback just send me a message here or on [twitter](http://twitter.com/vannicaruso) and if you want to contribute just send a pull request.

License
-------

Prequel is licensed under the [wtfpl](http://sam.zoy.org/wtfpl/).