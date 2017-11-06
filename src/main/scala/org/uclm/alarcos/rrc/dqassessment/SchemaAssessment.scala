package org.uclm.alarcos.rrc.dqassessment

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.uclm.alarcos.rrc.config.{DQAssessmentConfiguration, DQParametersConfiguration}
import org.uclm.alarcos.rrc.configDQ.SparkDQConfiguration
import org.uclm.alarcos.rrc.dataquality.completeness.SchemaCompletenessMeasurement
import org.uclm.alarcos.rrc.reasoning.Inference
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._

/**
  * Created by raulreguillo on 6/09/17.
  */
class SchemaAssessment(config: DQAssessmentConfiguration,
                       paramConfig: DQParametersConfiguration,
                       sparkSession: SparkSession, inputFile: String) extends StepTrait with SchemaCompletenessMeasurement with Inference{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    import processSparkSession.implicits._

    val today = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    val calculationDate = dateFormat.format(today)

    def setLevels = udf((value: Double)=>{
      if (value <= paramConfig.BAD)
        "BAD"
      else if (value <= paramConfig.NORMAL)
        "NORMAL"
      else
        "GOOD"
    })


    val loadGraphInit = System.currentTimeMillis()
    val graph = loadGraph(sparkSession, inputFile)
    graph.edges.count()
    val loadGraphTime = System.currentTimeMillis() - loadGraphInit

    //Check lazy evaluations
    val processTimeInit = System.currentTimeMillis()
    val result = applyRuleSet(getMeasurementSubgraph(graph.vertices, graph, config.properties),
      "measurement", "contextualAssessment", setLevels).toDF()
    result.show(10000, truncate = false)
    val processTime = System.currentTimeMillis() - processTimeInit

    val nNodes =  graph.vertices.count()
    val nEdges = graph.edges.count()

    val avgReadTimePerNode = loadGraphTime.toDouble/nNodes
    val avgReadTimePerEdge = loadGraphTime.toDouble/nEdges

    val avgProcessTimePerNode = processTime.toDouble/nNodes
    val avgProcessTimePerEdge = processTime.toDouble/nEdges

    val totalTime = loadGraphTime + processTime
    val statisticsDF = sparkSession.sparkContext.parallelize(Seq((
      calculationDate,
      "SchemaAssessment",
      loadGraphTime,
      processTime,
      totalTime,
      nNodes,
      nEdges,
      avgReadTimePerNode,
      avgReadTimePerEdge,
      avgProcessTimePerNode,
      avgProcessTimePerEdge))).toDF(Seq(
      "calculationDate",
      "metric",
      "loadGraphTime",
      "processTime",
      "totalTime",
      "nNodes",
      "nEdges",
      "avgReadTimePerNode",
      "avgReadTimePerEdge",
      "avgProcessTimePerNode",
      "avgProcessTimePerEdge"
    ): _*)

    statisticsDF.show(100, truncate=false)

    result.limit(100).coalesce(1).write.json(config.hdfsOutputPath + "SchemaAssessment/" + System.currentTimeMillis() + "/")
    statisticsDF.coalesce(1).write.json(config.hdfsOutputPath + "DQAssessmentStatistics/" + System.currentTimeMillis() + "/")
  }
}