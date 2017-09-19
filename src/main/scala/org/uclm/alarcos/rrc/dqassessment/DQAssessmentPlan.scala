package org.uclm.alarcos.rrc.dqassessment

import org.apache.spark.sql._
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.dataquality.completeness.InterlinkingMeasurement

/**
  * Created by raulreguillo on 6/09/17.
  */


class DQAssessmentPlan(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends InterlinkingMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    val s2 = getSubjectsWithProperty(graph, "http://dbpedia.org/ontology/deathPlace")
    s2.collect().foreach(println(_))
    var result = getMeasurementSubgraph(s2, graph, 3)
    result.show(100, truncate=false)
    //result.collect().foreach(println(_))
  }
}