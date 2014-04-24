package org.apache.spark.flare

import org.scalatest.FunSuite
import org.scalatest.Suite

class SimpleSuite extends Suite {
  
  def testOk(){
    assert(true)
  }
  
}

class SimpleFunSuite extends FunSuite {

  test("hi scalatest!") {
    val expr = "a > 100"
    val strings = expr.split(">")
    assert(strings.length == 2)
  }
  
}

