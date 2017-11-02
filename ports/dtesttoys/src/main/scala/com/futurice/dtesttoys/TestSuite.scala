package com.futurice.dtesttoys

import java.io.{BufferedReader, File, FileReader}

import fi.veikkaus.dcontext._
import com.futurice.testtoys._

import scala.collection.mutable.ArrayBuffer

class TestSuite(val name:String) extends Contextual(name) with ContextTask {

  type TestMethod = (MutableDContext, TestTool)=>Unit
  type TestCase = (String, TestMethod)

  val Fail = 0
  val Ok = 1
  val Quit = 2
  type Result = Int

  def rootPath(c:MutableDContext) = c.get(TestSuite.testPathKey).getOrElse("./io/test")

  val tests = ArrayBuffer[TestCase]()

  def test(name:String)(t:TestMethod) = {
    tests += name -> t
  }

  def runTest(c:MutableDContext, t:TestCase) = {
    val tt = new TestTool(new File(new File(rootPath(c), name), t._1).getPath)

    t._2(c, tt)

    var quit = false
    var res = tt.done(Seq(tt.diffToolAction(),("[q]uit", "q", (_, _, _) => {
      quit = true
      (false, false)
    })))
    (quit, res) match {
      case (true, _) => Quit
      case (false, true) => Ok
      case (false, false) => Fail
    }
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
    val s = selected.iterator
    var cont = true
    var ok = true
    val before = System.currentTimeMillis()
    while (cont && s.hasNext) {
      s.next._2(context) match {
        case Fail => ok = false
        case Quit => ok = false; cont = false
        case _ =>
      }
    }
    val time = System.currentTimeMillis() - before
    if (!ok) {
      System.out.println(f"$name(${args.mkString(", ")}) failed in $time ms")
    } else {
      System.out.println(f"$name(${args.mkString(", ")}) done in $time ms")
    }
  }

}

object TestSuite {
  /**
   * The root path, where the test files are located
   */
  def testPathKey = "dtesttoys.testpath"
}


