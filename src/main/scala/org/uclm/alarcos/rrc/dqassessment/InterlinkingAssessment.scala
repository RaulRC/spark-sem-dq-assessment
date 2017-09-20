package org.uclm.alarcos.rrc.dqassessment

import org.apache.spark.sql.SparkSession
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.dataquality.completeness.InterlinkingMeasurement

/**
  * Created by raulreguillo on 6/09/17.
  */

class InterlinkingAssessment(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends StepTrait with InterlinkingMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    var result = getMeasurementSubgraph(graph.vertices, graph, config.depth)
    result.show(100, truncate = false)
  }
}