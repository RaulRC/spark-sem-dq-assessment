package org.uclm.alarcos.rrc.dq

import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.uclm.alarcos.rrc.CommonTest
import org.uclm.alarcos.rrc.spark.SparkSpec

/**
  * Created by raul.reguillo on 14/09/17.
  */
@RunWith(classOf[JUnitRunner])
class MeasurementsTest extends CommonTest with SparkSpec with MockFactory {

  "Execute getMEasurementSugraph SchemaCompleteness" should "be succesfully" in {
    assert(true)
  }
}
