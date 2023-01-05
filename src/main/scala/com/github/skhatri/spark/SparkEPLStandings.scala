package com.github.skhatri.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType

case class HomeAwayScore(home: Int, away: Int, home_points: Int, away_points: Int) extends Serializable {
}

object SparkEPLStandings extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL to View EPL results")
    .config("spark.driver.host", "localhost")
//    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  //Round,Date,Team 1,FT,Team 2
  val files = Seq("epl2012-part1.csv", "epl2012-part2.csv", "epl2012-part3.csv", "epl2012-part4.csv")
    .map(fileName => s"src/main/resources/$fileName")

  spark.udf.register("home_away_score", (score: String) => {
    val parts = score.split("-")
    val home = parts(0).toInt
    val away = parts(1).toInt
    val (home_points, away_points) = if (home > away) (3, 0) else if (home == away) (1, 1) else (0, 3)
    HomeAwayScore(parts(0).toInt, parts(1).toInt, home_points, away_points)
  })


  def loadData(): DataFrame = {
    val df: DataFrame = spark.read.option("header", true)
      .csv(files: _*)
      .selectExpr("week", "match_date", "home", "score", "away")
      .selectExpr("*", "home_away_score(score) as match_score")

    df.show(5, false)
    df.printSchema()
    df
  }

  // Implementations below are tested with Spark 3.3.1

  def findTeamsInPremierLeague(df: DataFrame): Unit = {
    df.select(col("home").as("team")).union(df.select(col("away").as("team" )) )
      .distinct()
      .orderBy(col("team"))
      .show(false)
  }

  def findAllMatchesByTeam(df: DataFrame, name: String): Unit = {
    df.filter(col("home") === name or col("away") === name)
      .orderBy(to_date(col("match_date").substr(lit(5), length(col("match_date")) - 1), "MMM d yyyy"))
      .show(false)
  }

  val regexString = "(\\d+)-(\\d+)"

  def findBiggestWinByAnyTeam(df: DataFrame): Unit = {
    val windowSpec  = Window.orderBy(col("score_diff").desc )

    df
      .withColumn("home_score", regexp_extract(col("score"), regexString, 1).cast(IntegerType))
      .withColumn("away_score", regexp_extract(col("score"), regexString, 2).cast(IntegerType))
      .withColumn("score_diff", abs(col("home_score") - col("away_score")))
      .withColumn("winning_team", when(col("home_score") > col("away_score"), col("home")).otherwise(col("away")))
      .withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") === 1)
      .show(false)
  }

//  def findTeamTotalGoals(df: DataFrame, name: String): Int = {
//    val team_total_goals = df
//      .filter(col("home") === name or col("away") === name)
//      .withColumn("home_score", regexp_extract(col("score"), regexString, 1).cast(IntegerType))
//      .withColumn("away_score", regexp_extract(col("score"), regexString, 2).cast(IntegerType))
//      .withColumn("given_team_score", when(col("home") === name, col("home_score")).otherwise(col("away_score")) )
//      .select(sum(col("given_team_score")))
//      .first.getLong(0).toInt
//
//    println(s"$name total goals: " + team_total_goals)
//
//    return team_total_goals
//  }

  def findTeamsPoints(df: DataFrame, name: String): Int = {
    val team_total_points = df
      .filter(col("home") === name or col("away") === name)
      .withColumn("home_score", regexp_extract(col("score"), regexString, 1).cast(IntegerType))
      .withColumn("away_score", regexp_extract(col("score"), regexString, 2).cast(IntegerType))
      .withColumn("team_points", when(col("home") === name and col("home_score") > col("away_score"), lit(3))
        .when(col("away") === name and col("away_score") > col("home_score"), lit(3))
        .when(col("home_score") === col("away_score"), lit(1))
        .otherwise(lit(0)))
      .select(sum(col("team_points")))
      .first.getLong(0).toInt

    println(s"$name total points: " + team_total_points)

    team_total_points
  }

  val df: DataFrame = loadData()
  findTeamsInPremierLeague(df)
  /*
  +-------------------------+
  |team                     |
  +-------------------------+
  |AFC Bournemouth          |
  |AFC Wimbledon            |
  |Accrington Stanley FC    |
  |Aldershot Town FC        |
  |Arsenal FC               |
  |Aston Villa FC           |
  |Barnet FC                |
  |Barnsley FC              |
  |Birmingham City FC       |
  |Blackburn Rovers FC      |
  |Blackpool FC             |
  |Bolton Wanderers FC      |
  |Bradford City AFC        |
  |Brentford FC             |
  |Brighton & Hove Albion FC|
  |Bristol City FC          |
  |Bristol Rovers FC        |
  |Burnley FC               |
  |Burton Albion FC         |
  |Bury FC                  |
  +-------------------------+
  only showing top 20 rows
  */

  findAllMatchesByTeam(df, "Manchester United FC")
  /*
  +----+---------------+--------------------+-----+-----------------------+------------+
  |week|match_date     |home                |score|away                   |match_score |
  +----+---------------+--------------------+-----+-----------------------+------------+
  |1   |Mon Aug 20 2012|Everton FC          |1-0  |Manchester United FC   |{1, 0, 3, 0}|
  |2   |Sat Aug 25 2012|Manchester United FC|3-2  |Fulham FC              |{3, 2, 3, 0}|
  |3   |Sun Sep 2 2012 |Southampton FC      |2-3  |Manchester United FC   |{2, 3, 0, 3}|
  |4   |Sat Sep 15 2012|Manchester United FC|4-0  |Wigan Athletic FC      |{4, 0, 3, 0}|
  |5   |Sun Sep 23 2012|Liverpool FC        |1-2  |Manchester United FC   |{1, 2, 0, 3}|
  |6   |Sat Sep 29 2012|Manchester United FC|2-3  |Tottenham Hotspur FC   |{2, 3, 0, 3}|
  |7   |Sun Oct 7 2012 |Newcastle United FC |0-3  |Manchester United FC   |{0, 3, 0, 3}|
  |8   |Sat Oct 20 2012|Manchester United FC|4-2  |Stoke City FC          |{4, 2, 3, 0}|
  |9   |Sun Oct 28 2012|Chelsea FC          |2-3  |Manchester United FC   |{2, 3, 0, 3}|
  |10  |Sat Nov 3 2012 |Manchester United FC|2-1  |Arsenal FC             |{2, 1, 3, 0}|
  |11  |Sat Nov 10 2012|Aston Villa FC      |2-3  |Manchester United FC   |{2, 3, 0, 3}|
  |12  |Sat Nov 17 2012|Norwich City FC     |1-0  |Manchester United FC   |{1, 0, 3, 0}|
  |13  |Sat Nov 24 2012|Manchester United FC|3-1  |Queens Park Rangers FC |{3, 1, 3, 0}|
  |14  |Wed Nov 28 2012|Manchester United FC|1-0  |West Ham United FC     |{1, 0, 3, 0}|
  |15  |Sat Dec 1 2012 |Reading FC          |3-4  |Manchester United FC   |{3, 4, 0, 3}|
  |16  |Sun Dec 9 2012 |Manchester City FC  |2-3  |Manchester United FC   |{2, 3, 0, 3}|
  |17  |Sat Dec 15 2012|Manchester United FC|3-1  |Sunderland AFC         |{3, 1, 3, 0}|
  |18  |Sun Dec 23 2012|Swansea City FC     |1-1  |Manchester United FC   |{1, 1, 1, 1}|
  |19  |Wed Dec 26 2012|Manchester United FC|4-3  |Newcastle United FC    |{4, 3, 3, 0}|
  |20  |Sat Dec 29 2012|Manchester United FC|2-0  |West Bromwich Albion FC|{2, 0, 3, 0}|
  +----+---------------+--------------------+-----+-----------------------+------------+
  only showing top 20 rows
  */

  findTeamsPoints(df, "Arsenal FC")
  // Arsenal FC total points: 73

  findBiggestWinByAnyTeam(df)
  /*
  +----+---------------+----------+-----+--------------+------------+----------+----------+----------+------------+----+
  |week|     match_date|      home|score|          away| match_score|home_score|away_score|score_diff|winning_team|rank|
  +----+---------------+----------+-----+--------------+------------+----------+----------+----------+------------+----+
  |  18|Sun Dec 23 2012|Chelsea FC|  8-0|Aston Villa FC|{8, 0, 3, 0}|         8|         0|         8|  Chelsea FC|   1|
  +----+---------------+----------+-----+--------------+------------+----------+----------+----------+------------+----+
  */

}
