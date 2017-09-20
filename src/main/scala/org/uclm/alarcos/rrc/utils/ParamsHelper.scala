package org.uclm.alarcos.rrc.poc.utils

/**
  * Created by Raul Reguillo on 31/08/17.
  */

case class Params(inputClass: String, env: String, inputFile: String)

object ParamsHelper {
  def getParams(args: Array[String]): Params = {
    Params(args(0), args(1), args(2))
  }
}



