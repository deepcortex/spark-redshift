package com.databricks.spark.redshift

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

class HikariPoolProvider {
  private val conf = ConfigFactory.load()
  private val hikariConfig = new HikariConfig()

  val maxPoolSize = conf.getInt("spark-redshift-connector.hikari.max-pool-size")
  hikariConfig.setMaximumPoolSize(maxPoolSize)

  val jdbcUrl = conf.getString("spark-redshift-connector.redshift-jdbc-url")
  hikariConfig.setJdbcUrl(jdbcUrl)

  val username = conf.getString("spark-redshift-connector.db-username")
  hikariConfig.setUsername(username)

  val password = conf.getString("spark-redshift-connector.db-password")
  hikariConfig.setPassword(password)

  val initSql = conf.getString("spark-redshift-connector.hikari.connection-init-sql")
  hikariConfig.setConnectionInitSql(initSql)

  val driverClass = conf.getString("spark-redshift-connector.hikari.driver-class")
  hikariConfig.setDriverClassName(driverClass)

  val hikariDataSource: HikariDataSource = new HikariDataSource(hikariConfig)

  /**
    *
    * @param credentials
    * @return
    */
  def getConnection(credentials: Option[(String, String)]) = {
    credentials.fold(hikariDataSource.getConnection()) { case (username, password) =>
      hikariDataSource.getConnection(username, password)
    }
  }
}
