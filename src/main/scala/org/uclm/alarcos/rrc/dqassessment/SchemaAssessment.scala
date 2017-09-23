package org.uclm.alarcos.rrc.dqassessment

import org.apache.spark.sql.SparkSession
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.configDQ.SparkDQConfiguration
import org.uclm.alarcos.rrc.dataquality.completeness.SchemaCompletenessMeasurement
import org.uclm.alarcos.rrc.reasoning.Inference
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._

/**
  * Created by raulreguillo on 6/09/17.
  */
class SchemaAssessment(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends StepTrait with SchemaCompletenessMeasurement with Inference{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val x: SparkDQConfiguration = null
    val graph = loadGraph(sparkSession, inputFile)
    def setLevels = udf((value: Double)=>{
      if (value <=0.34)
        "http://myontology.org/result#BAD"
      else if (value <= 0.67)
        "http://myontology.org/result#NORMAL"
      else
        "http://myontology.org/result#GOOD"
    })

    var result = applyRuleSet(getMeasurementSubgraph(graph.vertices, graph, config.properties),
      "measurement", "contextualAssessment", setLevels).limit(10).toDF()
    result.show(100, truncate = false)
    val AWS_ACCESS = System.getenv("AWS_ACCESS_KEY_ID")
    val AWS_SECRET = System.getenv("AWS_SECRET_ACCESS_KEY")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AWS_ACCESS)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AWS_SECRET)
    result.coalesce(1).write.json(config.hdfsOutputPath + System.currentTimeMillis() + "/")
  }
}