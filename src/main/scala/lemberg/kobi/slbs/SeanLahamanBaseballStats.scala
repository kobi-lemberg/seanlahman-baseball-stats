package lemberg.kobi.slbs

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

class SeanLahamanBaseballStats(config: Config, sparkSession: SparkSession) {

  private val properties = {
    val prop = new Properties()
    prop.put("user", config.getString("lemberg.kobi.slbs.mysql.username"))
    prop.put("password", config.getString("lemberg.kobi.slbs.mysql.password"))
    prop
  }

  private val jdbcUrl = config.getString("lemberg.kobi.slbs.mysql.jdbc-url")

  def answerQ1(pitchers: DataFrame): DataFrame = {
    val salaries = readTable("Salaries").select("playerID", "yearID", "salary").cache()
    val infielders = readTable("Fielding").select("playerID", "yearID")

    //Calculate the average salary for infielders and pitchers for each year
    val pitchersYearlySalary = pitchers.drop("ERA", "W", "L").join(salaries, Seq("playerID", "yearID"), "inner")
      .groupBy("yearID").agg(avg("salary").as("Pitching"))

    val infieldersYearlySalary = infielders.join(salaries, Seq("playerID", "yearID"), "inner").groupBy("yearID").agg(avg("salary").as("Fielding"))

    //The assumption is that we would like inner join
    pitchersYearlySalary.join(infieldersYearlySalary, Seq("yearID"), "inner").orderBy(desc("yearID"))
      .withColumnRenamed("yearID", "Year").select("Year", "Fielding", "Pitching")


  }

  def answeQ2(pitchers: DataFrame): DataFrame = {
    val allStarts = readTable("AllstarFull").select("playerID", "GP")
    val hallOfFame = readTable("HallOfFame").select("playerID", "yearid")
    val allStartsAppearances = allStarts.where("GP > 0").groupBy("playerID").count().withColumnRenamed("count", "All Star Appearances")
    val hallOfFamePeatchers = hallOfFame.join(pitchers.drop("yearID", "W", "L"), Seq("playerID"), "inner")
      .groupBy("playerID").agg(
        avg("ERA").as("ERA"),
        min("yearid").as("yearid")
      )

    val secondAnswer = hallOfFamePeatchers.join(allStartsAppearances, Seq("playerID"), "inner")
      .select("playerID", "ERA", "All Star Appearances", "yearid")
      .withColumnsRenamed(Map("playerID" -> "Player", "yearid" -> "Hall of Fame Induction Year"))
    secondAnswer
  }


  def answerQ3(pitchers: DataFrame): DataFrame = {

    def groupByYeadAndPlayer(dataFrame: DataFrame): DataFrame = {
      dataFrame.groupBy("playerID", "yearID").agg(
          avg("ERA").as("ERA"),
          (sum("W") / (sum("W") + sum("L"))).as("Season W/L")
      ).withColumn(
        "Season W/L", when (col("Season W/L").cast(FloatType).isNull, 0.0).otherwise(col("Season W/L")) //Avoid null AVG, example hogsech01, 1934
      )
    }

    val postSeasonedPitchers = readTable("PitchingPost").select(
      col("playerID"),
      col("yearID"),
      when(col("ERA").cast(IntegerType).isNull, 0.0).otherwise(col("ERA")).as("ERA").cast(FloatType), //It seems that there are null values and ERA column is String (player ID 'hogsech01')
      col("W"),
      col("L")
    ).transform(groupByYeadAndPlayer).withColumnsRenamed(
      Map("ERA" -> "Post-season ERA", "Season W/L" -> "Post-season Win/Loss")
    )

    val groupedPitchers = pitchers.transform(groupByYeadAndPlayer).withColumnsRenamed(
      Map("ERA" -> "Regular Season ERA", "Season W/L" -> "Regular Season Win/Loss")
    )

    groupedPitchers.join(postSeasonedPitchers, Seq("playerID", "yearID"), "inner").orderBy(
      desc("Regular Season ERA"),
      desc("Post-season ERA"),
      desc("Regular Season Win/Loss"),
      desc("Post-season Win/Loss")
    ).select(
        col("yearID").as("Year"),
        col("playerID").as("Player"),
        col("Regular Season ERA"), col("Regular Season Win/Loss"), col("Post-season ERA"), col("Post-season Win/Loss"))
      .limit(10)

  }

  private def answerQ4 = {
    val teams = readTable("Teams").select(
      col("yearID").as("Year"),
      col("teamID").as("Team ID"),
      col("Rank"),
      col("AB").as("At Bats")
    ).cache()

    val yearWindow = Window.partitionBy("Year")
    val fourthAnswer = teams
      .withColumn("firstPlace", rank().over(yearWindow.orderBy(asc("Rank"))))
      .withColumn("lastPlace", rank().over(yearWindow.orderBy(desc("Rank"))))
      .where(expr("firstPlace = 1") || expr("lastPlace = 1"))
      .select("Team ID", "Year", "Rank", "At Bats")
    fourthAnswer
  }

  def readTable(tableName: String) = sparkSession.read.jdbc(url = jdbcUrl, table = tableName, properties = properties)

  def run(): Unit = {
    /***
     * Note; the repartition to 1 was meant to ensure we will get a single file (this is how I understand it)
     * I'm relaying on the native Hadoop writer, thus, I'm using sub folders
     *  -> we can do collect and than use boto to write to S3 to the same directory
     */
    val outputDir = config.getString("lemberg.kobi.slbs.stats.output-dir")
    val pitchers = readTable("Pitching").select("playerID", "yearID", "ERA", "W", "L").cache()

    val firstAnswer = answerQ1(pitchers = pitchers)
    firstAnswer.repartition(1).write.mode("overwrite").option("header","true").csv(s"$outputDir/1")

    val secondAnswer = answeQ2(pitchers = pitchers)
    secondAnswer.repartition(1).write.mode("overwrite").option("header","true").csv(s"$outputDir/2")

    val thirdAnswer = answerQ3(pitchers = pitchers)
    thirdAnswer.repartition(1).write.mode("overwrite").option("header","true").csv(s"$outputDir/3")

    val fourthAnswer = answerQ4
    fourthAnswer.repartition(1).write.mode("overwrite").option("header","true").csv(s"$outputDir/4")
  }

}
object SeanLahamanBaseballStats extends SparkProcess {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val sparkSession = createSparkSession(config.getConfig("lemberg.kobi.slbs.spark"))
    val runner = new SeanLahamanBaseballStats(config, sparkSession)
    runner.run()
  }
}
