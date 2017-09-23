package org.uclm.alarcos.rrc.dqassessment

import org.apache.spark.sql.SparkSession
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.dataquality.completeness.SchemaCompletenessMeasurement
import org.uclm.alarcos.rrc.io.ReaderRDF
import org.uclm.alarcos.rrc.statistics.StatisticsTrait

/**
  * Created by raulreguillo on 6/09/17.
  */
class StatisticsAssessment(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends StepTrait with ReaderRDF with StatisticsTrait{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    val numEdges = graph.edges.count()
    val numVertices = graph.vertices.count()
    val features = Seq("numVertices", "numEdges")
    val fValues = Seq((numVertices, numEdges))
    println("Edges: " + numEdges)
    println("Vertices: " + numVertices)
    import processSparkSession.implicits._
    val df = processSparkSession.sparkContext.parallelize(fValues).toDF(features: _*)
    df.show()

    /*println(graph.inDegrees)
    println(graph.outDegrees)
    println(graph.degrees)*/

  }
}