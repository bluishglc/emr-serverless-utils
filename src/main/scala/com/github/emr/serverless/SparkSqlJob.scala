package com.github.emr.serverless

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * A generic emr serverless job class for executing sql files.
 *
 * @author Laurence Geng
 */
object SparkSqlJob extends LazyLogging {
	def main(sysArgs: Array[String]) {

		val args = resolveArgs(sysArgs)

		import SparkSqlSupport._

		SparkSession
			.builder
			.enableHiveSupport()
			.getOrCreate()
		  .execSqlFile(args("sql-files"), args("sql-params"))
			.close()
	}
	private def resolveArgs(sysArgs: Array[String]): Map[String, Option[String]] = {
		val args = mutable.LinkedHashMap[String, Option[String]](
			"sql-files" -> None,
			"sql-params" -> None
		)

		sysArgs.sliding(2).foreach {
			case Array(l, r) if l.startsWith("--") && !r.startsWith("--") => {
				args.put(l.substring(2), Some(r))
			}
			case _ =>
		}

		logger.info(s"Resolved Args: ${args.mkString(", ")}")
		args.toMap
	}
}
