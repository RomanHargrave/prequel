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

      val poolerConfig = new HikariConfig()
      if (config.driver.length > 0 && config.jdbcURL.length > 0) {
        poolerConfig.setDriverClassName(config.driver)
        poolerConfig.setJdbcUrl(config.jdbcURL)
      } else {
        poolerConfig.setDataSourceClassName(config.dataSourceClassName)
      }
      poolerConfig.setMaximumPoolSize(config.maximumPoolSize)
      poolerConfig.setAutoCommit(config.autoCommit)

      config.properties.foreach {case(k, v) => poolerConfig.addDataSourceProperty(k, v) /* a tupleize function transform would be neat */}

     val ds = new HikariDataSource(poolerConfig)
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

