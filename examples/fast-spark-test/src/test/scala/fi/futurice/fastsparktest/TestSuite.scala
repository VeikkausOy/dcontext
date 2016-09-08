package fi.futurice.fastsparktest

import java.io.{BufferedReader, FileReader}

import fi.veikkaus.dcontext.{ContextTask, Contextual, MutableDContext, _}
import com.futurice.testtoys.{TestTool, _}
import org.apache.spark.sql.DataFrame

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

    tt.done()
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


  def ms[T](f: =>T) = {
    val before = System.currentTimeMillis()
    val rv = f
    (System.currentTimeMillis-before, rv)
  }
  def tLong[T](t:TestTool, relRange:Double, time:Long, unit:String) {
    val old = t.peekLong();
    t.i(f"$time $unit ")
    if (old != null) {
      if (relRange.isPosInfinity||Math.abs(Math.log(time/old.toDouble)) < Math.log(relRange)||old==0) {
        t.i(f"(was $old ms)")
      } else {
        t.t("(" +{((time*100)/old.toDouble).toInt}+ "% of old " + old + " ms)")
      }
    }
  }

  def tDouble[T](t:TestTool, relRange:Double, time:Double, unit:String) {
    val old = t.peekDouble();
    t.i(f"$time%.3f $unit ")
    if (old != null) {
      if (relRange.isPosInfinity||Math.abs(Math.log(time/old.toDouble)) < Math.log(relRange)||old==0) {
        t.i(f"(was $old%.3f $unit)")
      } else {
        t.t(f"(${((time*100)/old.toDouble).toInt}%% of old $old%.3f $unit)")
      }
    }
  }

  def tMs[T](t:TestTool, relRange:Double, f : => T) : T = {
    val (m, rv) = ms(f)
    tLong(t, relRange, m, "ms")
    rv
  }
  // allow the value to be twice as big or half as small
  def tMs[T](t:TestTool, f: => T) : T =
    tMs(t, 10, f)

  def iMs[T](t:TestTool, f: =>T) : T =
    tMs(t, Double.PositiveInfinity, f)

  def iMsLn[T](t:TestTool, f: =>T) : T = {
    val rv = tMs(t, Double.PositiveInfinity, f)
    t.i("\n")
    rv
  }

  def tFile(t:TestTool, f:java.io.File, lines:Int) {
    t.tln
    t.tln("file " + f.getName + " contents:")
    t.tln
    val r = new BufferedReader(new FileReader(f))
    try {
      (0 until lines).foreach { i =>
        t.tln("  " + r.readLine())
      }
      t.tln
    } finally {
      r.close
    }
  }

  def tDf(t:TestTool, df:DataFrame) {
    t.tln(df.count + " entries")
    t.tln(df.columns.size + " columns")
    t.tln
    t.tln(df.columns.map(e=>f"${e.toString.take(14)}%-14s").mkString("|"))
    df.take(8).foreach { l =>
      t.tln(l.toSeq.map(e => f"${(""+e).take(14)}%-14s").mkString("|"))
    }
    t.tln("...")
  }

}



