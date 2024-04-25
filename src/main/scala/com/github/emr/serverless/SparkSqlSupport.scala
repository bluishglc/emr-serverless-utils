package com.github.emr.serverless

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
 * SQL support class, add reading and executing sql files ability to spark via implicit conversion.
 *
 * @author Laurence Geng
 */
object SparkSqlSupport extends LazyLogging {

  implicit class SparkSqlEnhancer(spark: SparkSession) {

    def execSqlFile(sqlFilesOpt: Option[String], sqlParams: Option[String]): SparkSession = {
      val sqlFiles = sqlFilesOpt.get
      spark
        .sparkContext
        .wholeTextFiles(sqlFiles, 1)
        .values
        .collect
        .mkString(";")
        .split(";")
        .filter(StringUtils.isNotBlank(_))
        .foreach {
          sql =>
            val sqlBuilder = sqlParams.foldLeft(sql) {
              (sql, params) =>
                params.split(",").foldLeft(sql) {
                  (sql, kv) =>
                    val Array(key, value) = kv.split("=")
                    logger.info(s"Sql Param Key = $key, Sql Param Value = $value")
                    sql.replace(s"$${${key}}", value)
                }
            }
            logger.info(s"Sql to be executed: ${sqlBuilder}")
            spark.sql(sqlBuilder)
        }
      spark
    }

    def execSqlFile(sqlFilesOpt: String, sqlParams: String): SparkSession = {
      execSqlFile(Some(sqlFilesOpt), Some(sqlParams))
    }

    def execSqlFile(sqlFilesOpt: String): SparkSession = {
      execSqlFile(Some(sqlFilesOpt), None)
    }
  }

}
