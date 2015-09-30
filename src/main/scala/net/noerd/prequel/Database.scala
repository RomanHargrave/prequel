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

      val cpConfig = new HikariConfig()

      cpConfig.setMaximumPoolSize(config.maximumPoolSize)
      cpConfig.setAutoCommit(config.autoCommit)

      if (config.driver.length > 0 && config.jdbcURL.length > 0){
        cpConfig.setDriverClassName(config.driver)
        cpConfig.setJdbcUrl(config.jdbcURL)
      }else{
        cpConfig.setDataSourceClassName(config.dataSourceClassName)
      }

      config.properties.foreach {
        case(key, value) ⇒ cpConfig.addDataSourceProperty(key, value)
      }

     val ds = new HikariDataSource(cpConfig)
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

