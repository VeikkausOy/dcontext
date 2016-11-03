package fi.veikkaus.dcontext

import java.io.File

import com.futurice.testtoys.{TestTool, TestRunner}


object Tests extends Contextual("test") {
  def main(args:Array[String]): Unit = {
    TestRunner(
      "io/test",
      Seq( new DObjectTest ))
      .exec(args)
  }
  val workPath = cval("workPath") { c =>
    val rv = new File("io/tmp")
    rv.mkdirs()
    rv
  }

  def testContext(t:TestTool, c:MutableDContext = MutableDContext()) = {
    val tc = MutableDContext.mask(c)
    tc
  }

  def inTestContext(f:(TestTool,MutableDContext) => Unit) = {
    (t:TestTool) => {
      val c = testContext(t)
      f(t, c)
    }
  }
}