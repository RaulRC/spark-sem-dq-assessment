package org.uclm.alarcos.rrc

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.uclm.alarcos.rrc.config.{DQAssessmentConfiguration, DQParametersConfiguration}
import org.uclm.alarcos.rrc.poc.utils.ParamsHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.uclm.alarcos.rrc.dqassessment.StepTrait

/**
  * Created by Raul Reguillo on 31/08/17.
  */
object Main {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(getClass)
    logger.info("Main starting...")

    implicit val config: Config = ConfigFactory.load

    val environments = config.getStringList("environments")

    if (args.length == 0) {
      logger.error(s"Environment is mandatory. Valid environments are: $environments")
      System.exit(1)
    }
    implicit val params = ParamsHelper.getParams(args)
    implicit val env = params.env
    implicit val configFile = params.inputFile

    val paramConfig = DQParametersConfiguration.apply(ConfigFactory.parseFile(new File(configFile)))

    if (!environments.contains(env)) {
      logger.error(s"Environment $env not allowed. Valid environments are: $environments")
      System.exit(0)
    }

    val loadedClass = params.inputClass
    logger.info("Create Context for " + env)
    logger.info("Configuration file loaded..." + config.getConfig(env))

    val loadedConfig = DQAssessmentConfiguration.apply(env, config)
    implicit val inputFile =loadedConfig.hdfsInputPath
    val sparkConf = new SparkConf()
      .setAppName("DQAssessmentPlan")
      .setMaster(loadedConfig.masterMode)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val AWS_ACCESS = System.getenv("AWS_ACCESS_KEY_ID")
    val AWS_SECRET = System.getenv("AWS_SECRET_ACCESS_KEY")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AWS_ACCESS)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AWS_SECRET)

    logger.info("Loading class " + "DQAssessmentPlan")
    launchStep(Class.forName(s"org.uclm.alarcos.rrc.dqassessment.$loadedClass")) (loadedConfig, paramConfig, spark, inputFile)

  }

  /**
    * Launch a specific class
    *
    * @param clazz: Class
    * @param args: Arguments received
    * @tparam T
    * @return Launching classs
    */
  def launchStep[T](clazz: java.lang.Class[T])(args: AnyRef*): T = {
    val constructor = clazz.getConstructors()(0)
    val instance = constructor.newInstance(args: _*).asInstanceOf[T]
    instance.asInstanceOf[StepTrait].execute()
    instance
  }


}
