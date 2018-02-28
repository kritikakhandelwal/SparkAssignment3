package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Application extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("SparkAssignment").setMaster("local[*]")
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val filePath = "/home/knoldus/IdeaProjects/SparkAssignmnets3/src/main/resources/D1.csv"

  val obj = new Operation

  val matchDf = obj.createDataFrame(spark, filePath)
  matchDf.show()

  val hometeamMatchesDf = obj.homeTeamMatches(matchDf)
  hometeamMatchesDf.show()

  val totalMatch = obj.totalMatchCount(matchDf)
  val teamWin = obj.winMatchCount(matchDf)
  val df = obj.top10team(totalMatch, teamWin)
  df.show()

  import spark.implicits._

  /** Convert the DataFrame created above to DataSet by using only following fields:
    * HomeTeam, AwayTeam, FTHG, FTAG and FTR.
    */
  val matchDataSet: Dataset[Match] = matchDf.map(row => Match(row.getString(2), row.getString(3), row.getInt(4), row.getInt(5), row.getString(6)))
  matchDataSet.show()

  val matchCount = obj.matchCount(matchDataSet)
  matchCount.show()

  val top10Team = obj.top10Teamsmatch(matchDataSet)
  top10Team.show()

}
