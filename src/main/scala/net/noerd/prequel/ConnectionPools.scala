package net.noerd.prequel

import java.util.Properties

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.HashMap

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import javax.sql.DataSource


object ConnectionPools {

  private val pools: MMap[DatabaseConfig, DataSource] = new HashMap

  def nbrOfPools = pools.size

  def getOrCreatePool(config: DatabaseConfig): DataSource = pools.synchronized {
    pools.getOrElse(config, {

      val _config = new HikariConfig()
      _config.setMaximumPoolSize(config.maximumPoolSize)
      if (config.driver.length > 0 && config.jdbcURL.length > 0){
        _config.setDriverClassName(config.driver)
        _config.setJdbcUrl(config.jdbcURL)
      }else{
        _config.setDataSourceClassName(config.dataSourceClassName)
        _config.addDataSourceProperty("serverName", config.serverName)
        _config.addDataSourceProperty("serverPort", config.serverPort)
        _config.addDataSourceProperty("databaseName", config.databaseName)
      }
      _config.setAutoCommit(config.autoCommit)
      _config.addDataSourceProperty("user", config.username)
      _config.addDataSourceProperty("password", config.password)

     val ds = new HikariDataSource(_config)
      pools += ((config, ds))
      ds
    })
  }

  // Conversion method to deal with the nasty java.util.Properties class
  private def mapAsProperties(aMap: Map[String, String]): Properties = {
    val properties = new Properties
    aMap.map(pair => properties.setProperty(pair._1, pair._2))
    properties
  }

  // Used during testing
  private[prequel] def reset(): Unit = pools.clear
}

