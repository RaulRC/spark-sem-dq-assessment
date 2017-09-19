package org.uclm.alarcos.rrc

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.utils.ParamsHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
    implicit val inputFile = params.inputFile

    if (!environments.contains(env)) {
      logger.error(s"Environment $env not allowed. Valid environments are: $environments")
      System.exit(0)
    }

    logger.info("Create Context for " + env)
    logger.info("Configuration file loaded..." + config.getConfig(env))

    val loadedConfig = DQAssessmentConfiguration.apply(env, config)

    val sparkConf = new SparkConf()
      .setAppName("DQAssessmentPlan")
      .setMaster(loadedConfig.masterMode)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()


    logger.info("Loading class " + "DQAssessmentPlan")
    launchStep(Class.forName(s"org.uclm.alarcos.rrc.dqassessment.DQAssessmentPlan")) (loadedConfig, spark, inputFile)

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
    instance.asInstanceOf[org.uclm.alarcos.rrc.dqassessment.DQAssessmentPlan].execute()
    instance
  }


}