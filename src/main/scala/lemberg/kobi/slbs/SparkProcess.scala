package lemberg.kobi.slbs

import com.typesafe.config.{Config, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

/**
 * This trait is used to create a spark session
 */
trait SparkProcess {

  /**
   * Create a spark session
   * @param conf the configuration, should contain master and app-name
   * @return a spark session
   */
  def createSparkSession(conf: Config): SparkSession = {
    val requiredSparkConf: SparkConf = new SparkConf()
      .setMaster(conf.getString("master"))
      .setAppName(conf.getString("app-name"))

    val sparkConf = conf.getConfig("process-config").entrySet().asScala.foldLeft(requiredSparkConf) { case (sparkConf, entry) =>
      sparkConf.set(entry.getKey,extractConfigValue(entry))
    }
    SparkSession.builder.config(sparkConf).getOrCreate()
  }

  /**
   * Add a custom config to the spark session
   * E.g spark.sql.shuffle.partitions, spark.executor.cores...
   * the entries should be entered within process-config section in the configuration file
   */
  def extractConfigValue(entry: java.util.Map.Entry[String, ConfigValue]): String = entry.getValue.render.replace("\"", "")

}
