package org.uclm.alarcos.rrc.dqassessment

import org.apache.spark.sql._
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.dataquality.completeness.{InterlinkingMeasurement, SchemaCompletenessMeasurement}

/**
  * Created by raulreguillo on 6/09/17.
  */


class DQAssessmentPlan(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends SchemaCompletenessMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    var result = getMeasurementSubgraph(graph.vertices, graph, config.properties)
    result.show(100, truncate = false)
  }
}