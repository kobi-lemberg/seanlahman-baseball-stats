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

  /**
   * afterStart - test containers hook to configure the container after startup - here we create the schema
   * for some reason MySql refuses to create multiple tables in one statement
   * I think this is something with the Driver version
   * hence the split and the loop
   */
  override def afterStart(): Unit = {
    super.afterStart()
    Class.forName("com.mysql.cj.jdbc.Driver")
    val createStatementPath = this.getClass.getResource("/create_schema.sql").toURI
    scala.io.Source.fromFile(createStatementPath).getLines().mkString("\n").split("--\n").foreach(runSql(_)())
  }

  /**
   * Before each test we truncate all tables
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    runSql("truncate Pitching")()
    runSql("truncate PitchingPost")()
    runSql("truncate Fielding")()
    runSql("truncate Salaries")()
    runSql("truncate AllstarFull")()
    runSql("truncate AllstarFull")()
  }

  /**
   * After all tests we stop the spark session
   */
  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }

  /**
   * Run a sql statement
   * @param sql the sql statement
   * @param validateAffected a function to validate the number of affected rows
   */
  def runSql(sql: String)(validateAffected: Int => Unit = { _ => ()}): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val connection = DriverManager.getConnection(mySQLContainer.jdbcUrl, mySQLContainer.username, mySQLContainer.password)
    val statement = connection.createStatement()
    validateAffected(statement.executeUpdate(sql))
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
      runSql("""
               |INSERT INTO Pitching (playerID, yearID, teamID, W, L, G, ERA) VALUES
               |                       ('a',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('a',    1999,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    1999,  'team1', 10, 1, 20, 2.0);
               |""".stripMargin) { _ shouldBe 4}
      runSql("""
               |INSERT INTO Fielding (playerID, yearID) VALUES
               |                       ('c',    2000),
               |                       ('d',    2000),
               |                       ('c',    1999),
               |                       ('d',    1999);
               |""".stripMargin) { _ shouldBe 4}
      runSql("""
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

      val answerQ1 = runner.answerQ1(pitchers = runner.readTable("Pitching"))

      val fields = Seq("year", "Fielding", "Pitching")
      val expected = Seq(
        (2000, 3.5f, 1.5f),
        (1999, 3.5f, 1.5f)
      ).toDF(fields: _*)
      val result = answerQ1.join(expected, fields, "left_anti").count()
      result shouldBe 0
    }

    it("should be able to answer question 2") {
      runSql(
        """
          |INSERT INTO AllstarFull (playerID, GP) VALUES ('a', 1), ('a', 1), ('b', 1);
          |""".stripMargin) { _ shouldBe 3}
      runSql(
        """
          |INSERT INTO HallOfFame (playerID, yearid) VALUES ('a', 1999), ('a', 2000), ('b', 2000);
          |""".stripMargin) { _ shouldBe 3}
      runSql("""
               |INSERT INTO Pitching (playerID, yearID, teamID, W, L, G, ERA) VALUES
               |                       ('a',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('a',    1999,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    1999,  'team1', 10, 1, 20, 2.0);
               |""".stripMargin) { _ shouldBe 4}

      val answerQ2 = runner.answerQ2(pitchers = runner.readTable("Pitching"))

      val fields = Seq("Player", "ERA", "All Star Appearances", "Hall of Fame Induction Year")
      val expected = Seq(
        ("b", 2.0f, 1, 2000),
        ("a", 2.0f, 2, 1999)
      ).toDF(fields: _*)
      val result = answerQ2.join(expected, fields, "left_anti").count()
      result shouldBe 0
    }

    it("should be able to answer question 3") {
      runSql("""
               |INSERT INTO Pitching (playerID, yearID, teamID, W, L, G, ERA) VALUES
               |                       ('a',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('a',    1999,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    1999,  'team1', 10, 1, 20, 2.0);
               |""".stripMargin) { _ shouldBe 4}

      runSql("""
               |INSERT INTO PitchingPost (playerID, yearID, teamID, W, L, G, ERA) VALUES
               |                       ('a',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    2000,  'team1', 10, 1, 20, 2.0),
               |                       ('a',    1999,  'team1', 10, 1, 20, 2.0),
               |                       ('b',    1999,  'team1', 10, 1, 20, 2.0);
               |""".stripMargin) { _ shouldBe 4}

      val answerQ3 = runner.answerQ3(pitchers = runner.readTable("Pitching"))

      val fields = Seq("Year", "Player", "Regular Season ERA", "Regular Season Win/Loss", "Post-season ERA", "Post-season Win/Loss")
      val expected = Seq(
        (1999, "b", 2.0, 0.9090909090909091, 2.0, 0.9090909090909091),
        (2000, "a", 2.0, 0.9090909090909091, 2.0, 0.9090909090909091),
        (1999, "a", 2.0, 0.9090909090909091, 2.0, 0.9090909090909091),
        (2000, "b", 2.0, 0.9090909090909091, 2.0, 0.9090909090909091)
      ).toDF(fields: _*)
      val results = answerQ3.join(expected, fields, "left_anti").collect()
      results.length shouldBe 0
    }

    it("should be able to answer question 4") {
      runSql("""
               |INSERT INTO Teams (yearID, teamID, `Rank`, AB) VALUES
               |                   (2000, 't1',    1,    10),
               |                   (2000, 't2',    2,    20),
               |                   (2000, 't3',    3,    30),
               |                   (1999, 't4',    1,    40),
               |                   (1999, 't5',    2,    50),
               |                   (1999, 't6',    3,    60);
               |""".stripMargin) { _ shouldBe 6}

      val answerQ4 = runner.answerQ4

      val fields = Seq("Team ID", "Year", "Rank", "At Bats")
      val expected = Seq(
        ("t1", 2000, 1, 10),
        ("t3", 2000, 3, 30),
        ("t4", 1999, 1, 40),
        ("t6", 1999, 3, 60)
      ).toDF(fields: _*)
      val results = answerQ4.join(expected, fields, "left_anti").collect()
      results.length shouldBe 0
    }
  }


  override def container: Container = mySQLContainer
}