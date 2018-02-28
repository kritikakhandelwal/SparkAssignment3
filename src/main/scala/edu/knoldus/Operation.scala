package edu.knoldus

import edu.knoldus.Application.spark
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

class Operation {

  /** Read the input CSV file using Spark Session and create a DataFrame.
    */
  def createDataFrame(spark: SparkSession, path: String): DataFrame = {

    val matchDf = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    matchDf
  }

  /** Total number of match played by each team as HOME TEAM.
    */
  def homeTeamMatches(df: DataFrame): DataFrame = {

    df.createOrReplaceTempView("HomeMatches")
    val homeTeamMatchesDf = spark.sql("SELECT HomeTeam,count(FTR)  FROM HomeMatches " +
      " Group By HomeTeam")
    homeTeamMatchesDf
  }

  /** Q3 **/

  def totalMatchCount(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("HomeMatches")
    val homeMatches = spark.sql("SELECT HomeTeam AS TeamName,count(HomeTeam) AS TotalMatch From HomeMatches Group By HomeTeam")
    val awayMatches = spark.sql("SELECT HomeTeam AS TeamName,count(HomeTeam)  AS TotalMatch From HomeMatches Group By HomeTeam")
    val totalMatch = homeMatches.union(awayMatches).createOrReplaceTempView("totalMatchView")
    val total = spark.sql("SELECT TeamName,sum(TotalMatch) AS TotalMatches FROM totalMatchView Group By TeamName")
    total
  }

  def winMatchCount(df: DataFrame): DataFrame = {

    val HomeDf = spark.sql("SELECT HomeTeam AS TeamName,count(FTR) AS Count From HomeMatches " +
      "WHERE FTR='H' Group By HomeTeam")

    val AwayDf = spark.sql("SELECT AwayTeam AS TeamName,count(FTR) AS Count From HomeMatches " +
      "WHERE FTR='A' Group By AwayTeam")

    val df = HomeDf.union(AwayDf).createOrReplaceTempView("MATCHES")

    val newDf = spark.sql("SELECT TeamName,sum(Count) AS WinMatch FROM MATCHES Group By TeamName")
    newDf
  }


  /** Top 10 team with highest wining percentage.
    */
  def top10team(totalMatch: DataFrame, winMatch: DataFrame): DataFrame = {
    val df = totalMatch.join(winMatch, "TeamName").createOrReplaceTempView("TopTeams")

    val top10teamdf = spark.
      sql("SELECT TeamName,TotalMatches,WinMatch, (WinMatch/TotalMatches)*100 as WinPercentage FROM TopTeams  ORDER BY WinPercentage DESC limit 10")
    top10teamdf
  }


  def matchCount(matchDataSet: Dataset[Match]): DataFrame = {
    val matchesCount: DataFrame = matchDataSet.select("HomeTeam").withColumnRenamed("HomeTeam", "Team")
      .union(matchDataSet.select("AwayTeam").withColumnRenamed("AwayTeam", "Team")).groupBy("Team").count()
      .withColumnRenamed("count", "TotalMatches")
    matchesCount

  }

  def top10Teamsmatch(matchDataSet: Dataset[Match]): Dataset[(String, BigInt)] = {
    import spark.implicits._
    val homeTeamDF = matchDataSet.select("HomeTeam", "FTR").where("FTR = 'H'").groupBy("HomeTeam").
      count().withColumnRenamed("count", "Wins").withColumnRenamed("HomeTeam", "Teams")
    val awayTeamDF = matchDataSet.select("AwayTeam", "FTR").where("FTR = 'A'").groupBy("AwayTeam").
      count().withColumnRenamed("count", "Wins").withColumnRenamed("AwayTeam", "Teams")
    val teamsDF: DataFrame = homeTeamDF.union(awayTeamDF).groupBy("Teams").sum("Wins")
    teamsDF.as[(String, BigInt)]
  }
}
