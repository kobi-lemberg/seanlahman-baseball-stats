package lemberg.kobi.slbs

import com.dimafeng.testcontainers.{Container, ForAllTestContainer, MySQLContainer}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.testcontainers.shaded.org.apache.commons.io.FileUtils

import java.sql.DriverManager

//noinspection SourceNotClosed
class SeanLahamanBaseballStatsSpec extends AnyFunSpec
  with Matchers with ForAllTestContainer with BeforeAndAfterAll with BeforeAndAfterEach {



  private lazy val mySQLContainer: MySQLContainer = MySQLContainer(mysqlImageVersion = "mysql:8.0" )
  private lazy val spark: SparkSession = new SparkSession.Builder().appName("tests").master("local").getOrCreate()
  import spark.implicits._
  private val tempDir = FileUtils.getTempDirectoryPath
  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }

  override def afterStart(): Unit = {
    super.afterStart()
    Class.forName("com.mysql.cj.jdbc.Driver")
    val connection = DriverManager.getConnection(mySQLContainer.jdbcUrl, mySQLContainer.username, mySQLContainer.password)
    val createStatementPath = this.getClass.getResource("/create_schema.sql").toURI
    scala.io.Source.fromFile(createStatementPath).getLines().mkString("\n").split("--\n").foreach{ s =>
      connection.createStatement().executeUpdate(s)
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    insert("truncate Pitching")()
    insert("truncate Fielding")()
    insert("truncate Salaries")()
    insert("truncate AllstarFull")()

  }

  def insert(sql: String)(f: Int => Unit = {_ => ()}): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val connection = DriverManager.getConnection(mySQLContainer.jdbcUrl, mySQLContainer.username, mySQLContainer.password)
    val statement = connection.createStatement()
    f(statement.executeUpdate(sql))
    connection.close()
  }

  lazy val config: Config = ConfigFactory.parseString(s"""
       |lemberg.kobi.slbs {
       |  spark.master = "local"
       |  stats.output-dir = ${tempDir}/tmp/solution
       |  mysql {
       |    jdbc-url = "${mySQLContainer.jdbcUrl}"
       |    username = "${mySQLContainer.username}"
       |    password = "${mySQLContainer.password}"
       |  }
       |}
       |""".stripMargin).withFallback(ConfigFactory.load())


  lazy val runner = new SeanLahamanBaseballStats(config, spark)

  describe("SeanLahamanBaseballStats") {
    it("should be able to answer question 1") {
      insert("""
               |INSERT INTO Pitching (playerID, yearID, teamID, W, L, G, ERA) VALUES
               |                       ('a',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('a',    1999,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    1999,  'team1', 10, 1, 20, 2.0);
               |""".stripMargin) { _ shouldBe 4}

      insert("""
               |INSERT INTO Fielding (playerID, yearID) VALUES
               |                       ('c',    2000),
               |                       ('d',    2000),
               |                       ('c',    1999),
               |                       ('d',    1999);
               |""".stripMargin) { _ shouldBe 4}

      insert("""
               |INSERT INTO Salaries (yearID, teamID, playerID, salary) VALUES
               |                     ('2000', 'team1',  'a',         1),
               |                     ('2000', 'team1',  'b',         2),
               |                     ('2000', 'team1',  'c',         3),
               |                     ('2000', 'team1',  'd',         4),
               |                     ('1999', 'team1',  'a',         1),
               |                     ('1999', 'team1',  'b',         2),
               |                     ('1999', 'team1',  'c',         3),
               |                     ('1999', 'team1',  'd',         4);
               |""".stripMargin) { _ shouldBe 8}

      val fields = Seq("year", "Fielding", "Pitching")
      val res = runner.answerQ1(pitchers = runner.readTable("Pitching")).join(
        Seq(
          (2000, 3.5f, 1.5f),
          (1999, 3.5f, 1.5f)
        ).toDF(fields: _*), fields, "left_anti"
      ).count()

      res shouldBe 0
    }

    it("should be able to answer question 2") {
      insert(
        """
          |INSERT INTO AllstarFull (playerID, GP) VALUES ('a', 1), ('a', 1), ('b', 1);
          |""".stripMargin) { _ shouldBe 3}

      insert(
        """
          |INSERT INTO HallOfFame (playerID, yearid) VALUES ('a', 1999), ('a', 2000), ('b', 2000);
          |""".stripMargin) { _ shouldBe 3}

      insert("""
               |INSERT INTO Pitching (playerID, yearID, teamID, W, L, G, ERA) VALUES
               |                       ('a',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('a',    1999,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    1999,  'team1', 10, 1, 20, 2.0);
               |""".stripMargin) { _ shouldBe 4}

      val fields = Seq("Player", "ERA", "All Star Appearances", "Hall of Fame Induction Year")
      val results = runner.answeQ2(pitchers = runner.readTable("Pitching")).join(
        Seq(
          ("b", 2.0f, 1, 2000),
          ("a", 2.0f, 2, 1999)
        ).toDF(fields: _*), fields, "left_anti"
      ).collect()

      results.length shouldBe 0
    }

  }


  override def container: Container = mySQLContainer
}