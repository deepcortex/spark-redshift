package com.databricks.spark.redshift

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

class HikariPoolProvider {
  private val conf = ConfigFactory.load()
  private val hikariConfig = new HikariConfig()
  private val log = LoggerFactory.getLogger(getClass)

  val maxPoolSize = conf.getInt("spark-redshift-connector.hikari.max-pool-size")
  val jdbcUrl = conf.getString("spark-redshift-connector.redshift-jdbc-url")
  val username = conf.getString("spark-redshift-connector.db-username")
  val password = conf.getString("spark-redshift-connector.db-password")
  val initSql = conf.getString("spark-redshift-connector.hikari.connection-init-sql")
  val driverClass = conf.getString("spark-redshift-connector.hikari.driver-class")

  log.info(s"initializing hikari pool with params -" +
    s" maxPoolSize: $maxPoolSize," +
    s" jdbcUrl: $jdbcUrl," +
    s" username: $username," +
    s" password: $password," +
    s" initSql: $initSql," +
    s" driverClass: $driverClass")

  hikariConfig.setMaximumPoolSize(maxPoolSize)
  hikariConfig.setJdbcUrl(jdbcUrl)
  hikariConfig.setUsername(username)
  hikariConfig.setPassword(password)
  hikariConfig.setConnectionInitSql(initSql)
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
