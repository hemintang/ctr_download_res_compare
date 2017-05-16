package com.xm4399.service

import com.xm4399.util.DateKeyUtil
import org.apache.spark.sql.SparkSession

/**
  * Created by hemintang on 17-5-12.
  */
object CompareCTRWithDownload {

  val threshold: Double = 0.95
  var datekey: String = _

  val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
//    .master("local")
    .getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit ={
    if(1 == args.length){
      val daysAgo = args(0).toInt
      datekey = DateKeyUtil.getDatekey(daysAgo)
      for(position <- 1 to 10){
        run(position)
      }
    }
  }

  def run(position: Int): Unit ={
    //取出0-5测试组，位置在position的数据
    val dataDF0_5 = spark.sql(s"select query, gameId from datamarket_adgame.reality_search_show where datekey = '$datekey' and testid <=5 and testid >= 0 and position = $position")
    //取出0-5测试组，位置在position的数据
    val dataDF6_11 = spark.sql(s"select query, gameId from datamarket_adgame.reality_search_show where datekey = '$datekey' and testid <=11 and testid >= 6 and position = $position")

    //查询query， 游戏gameId出现在位置position的次数
    val queryGameId_count0_5 = dataDF0_5.groupBy("query", "gameId").agg("*" -> "count").toDF("query", "gameId0_5", "count0_5")
    val queryGameId_count6_11 = dataDF6_11.groupBy("query", "gameId").agg("*" -> "count").toDF("query", "gameId6_11", "count6_11")

    //查询query，出现在position的游戏次数
    val query_sum0_5 = dataDF0_5.groupBy("query").agg("*" -> "count").toDF("query", "sum0_5")
    val query_sum6_11 = dataDF6_11.groupBy("query").agg("*" -> "count").toDF("query", "sum6_11")

    val queryGameId_count_sum_rate0_5 = queryGameId_count0_5.join(query_sum0_5, Seq("query"), "left_outer").withColumn("rate0_5", $"count0_5"/$"sum0_5")
    val queryGameId_count_sum_rate6_11 = queryGameId_count6_11.join(query_sum6_11, Seq("query"), "left_outer").withColumn("rate6_11", $"count6_11"/$"sum6_11")

    //过滤出rate=coun/sum > 0.95的
    val queryGameId_count_sum_rate_filter0_5 = queryGameId_count_sum_rate0_5.where(s"rate0_5 > $threshold")
    val queryGameId_count_sum_rate_filter6_11 = queryGameId_count_sum_rate6_11.where(s"rate6_11 > $threshold")

    //ab组join
    val joinedDF = queryGameId_count_sum_rate_filter0_5.join(queryGameId_count_sum_rate_filter6_11, Seq("query"))

    //过滤出gameId不相等的
    val joinedDF_filter = joinedDF.where("gameId0_5 != gameId6_11")

    joinedDF_filter.createOrReplaceTempView("t_df")
    spark.sql(
      s"""
        |insert overwrite table default.t_ctr_download_compare
        |partition(datekey = '$datekey', position = $position)
        |select * from t_df
      """.stripMargin)

  }

  //计算当两种排序不一样的时候，用户的点击次数
  private def diffSortNumClick(position: Int): Unit ={

    val query0_5 = spark.sql(
      s"""
         |select query, gameid0_5 from default.t_ctr_download_compare
         |where datekey = '$datekey' and position = $position
       """.stripMargin)

    val query6_11 = spark.sql(
      s"""
         |select query, gameid6_11 from default.t_ctr_download_compare
         |where datekey = '$datekey' and position = $position
       """.stripMargin)

    val dataDF0_5 = spark.sql(
      s"""
         |select query, gameid, isclick from datamarket_adgame.reality_search_show
         |where datekey = '$datekey' and position = $position and testid >=0 and testid <= 5
       """.stripMargin).toDF("query", "gameid0_5", "isclick")

    val dataDF6_11 = spark.sql(
      s"""
         |select query, gameid, isclick from datamarket_adgame.reality_search_show
         |where datekey = '$datekey' and position = $position and testid >=6 and testid <= 11
       """.stripMargin).toDF("query", "gameid6_11", "isclick")
    dataDF0_5.join(query0_5, Seq("query", "gameid0_5")).agg("isclick" -> "sum").show()
    dataDF6_11.join(query6_11, Seq("query", "gameid6_11")).agg("isclick" -> "sum").show()
  }
}
