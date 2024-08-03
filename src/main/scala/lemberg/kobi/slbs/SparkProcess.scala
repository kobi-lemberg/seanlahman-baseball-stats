package lemberg.kobi.slbs

import com.typesafe.config.{Config, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

trait SparkProcess {

  //TODO "injectable" config
  def createSparkSession(conf: Config): SparkSession = {
    val requiredSparkConf: SparkConf = new SparkConf()
      .setMaster(conf.getString("master"))
      .setAppName(conf.getString("app-name"))

    val sparkConf = conf.getConfig("process-config").entrySet().asScala.foldLeft(requiredSparkConf) { case (sparkConf, entry) =>
      sparkConf.set(entry.getKey,extractConfigValue(entry))
    }
    SparkSession.builder.config(sparkConf).getOrCreate()
  }

  def extractConfigValue(entry: java.util.Map.Entry[String, ConfigValue]): String = entry.getValue.render.replace("\"", "")

}
