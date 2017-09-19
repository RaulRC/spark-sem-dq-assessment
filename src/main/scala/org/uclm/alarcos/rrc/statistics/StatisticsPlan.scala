package org.uclm.alarcos.rrc.statistics

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by raulreguillo on 19/09/17.
  */

trait StatisticsTrait extends Serializable {
  protected val processSparkSession: SparkSession

  def getTotalNodes(df: DataFrame): DataFrame = {
    import processSparkSession.implicits._
    df.select("source").distinct()
  }
}