package org.uclm.alarcos.rrc.dqassessment

import org.apache.spark.sql.SparkSession
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.configDQ.SparkDQConfiguration
import org.uclm.alarcos.rrc.dataquality.completeness.SchemaCompletenessMeasurement


/**
  * Created by raulreguillo on 6/09/17.
  */
class SchemaAssessment(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends StepTrait with SchemaCompletenessMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val x: SparkDQConfiguration = null
    val graph = loadGraph(sparkSession, inputFile)
    var result = getMeasurementSubgraph(graph.vertices, graph, config.properties)
    result.show(100, truncate = false)
  }
}