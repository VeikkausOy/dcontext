package com.futurice.dtesttoys

import java.io.{BufferedReader, FileReader}

import fi.veikkaus.dcontext._
import com.futurice.testtoys._

import scala.collection.mutable.ArrayBuffer

abstract class TestSuite(val name:String) extends Contextual(name) with ContextTask {

  type TestMethod = (MutableDContext, TestTool)=>Unit
  type TestCase = (String, TestMethod)

  val tests = ArrayBuffer[TestCase]()

  def test(name:String)(t:TestMethod) = {
    tests += name -> t
  }

  def runTest(c:MutableDContext, t:TestCase) = {
    val tt = new TestTool("io/test/" + name + "/" + t._1)

    t._2(c, tt)

    tt.done
  }

  def testOps =
    tests.map( t =>
      (t._1, (c:MutableDContext) => runTest(c, t)))


  def ops : Seq[(String, (MutableDContext) => AnyVal)] = {
    testOps ++
    Array(("-l",
      (c:MutableDContext) =>
       testOps.foreach { t =>
         System.out.println(t._1)
       }))
  }

  override def run(context: MutableDContext, args: Array[String]): Unit = {
    val selected =
      if (args.size == 0) {
        testOps
      } else {
        ops.filter{ case (name, op) => args.contains(name) }
      }
    selected.foreach(_._2(context))
  }


}


