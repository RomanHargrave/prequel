package net.noerd.prequel 

object TestDatabase {
    
    val config = DatabaseConfig(
        driver = "org.hsqldb.jdbc.JDBCDriver",
        jdbcURL = "jdbc:hsqldb:mem:mymemdb",
      //dataSourceClassName = "org.hsqldb.jdbc.JDBCDataSource",
      //serverName="localhost",
      sqlFormatter = SQLFormatter.HSQLDBSQLFormatter,
     // autoCommit = false,
    maximumPoolSize = 1
    )
}