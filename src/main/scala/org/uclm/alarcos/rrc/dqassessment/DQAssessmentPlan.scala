package org.uclm.alarcos.rrc.dqassessment

import org.apache.spark.sql._
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.dataquality.completeness.{InterlinkingMeasurement, SchemaCompleteness, SchemaCompletenessgMeasurement}

/**
  * Created by raulreguillo on 6/09/17.
  */


class DQAssessmentPlan(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends SchemaCompletenessgMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    val s2 = getSubjectsWithProperty(graph, "http://dbpedia.org/ontology/deathPlace")
    s2.collect().foreach(println(_))
    var result = getMeasurementSubgraph(s2, graph, config.properties)
    result.show(100, truncate=false)
    //result.collect().foreach(println(_))
  }
}