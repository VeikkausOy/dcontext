package fi.veikkaus.dcontext

import java.io.File

import com.futurice.testtoys.{TestSuite, TestTool}
import fi.veikkaus.dcontext.dobject.{DObject, FsDObject}
import fi.veikkaus.dcontext.store.IoUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Created by arau on 2.11.2016.
  */
class DObjectTest extends TestSuite("dobject") {

  def cut(s: String, n: Int) = s.take(n) + (if (s.size > n) "..." else "")

  def tContext(t: TestTool, c: DContext) = {
    t.tln("context: ")
    c.keySet.toArray.sorted.foreach { key =>
      t.tln(f"  $key%-20s ${cut(c.get(key).get.toString, 24)}")
    }
  }

  def tDir(t: TestTool, f: File) = {
    t.tln("dir: ")
    f.listFiles().sorted.foreach { file =>
      t.tln(f"  ${file.getName}%-20s ${cut(IoUtil.readAny(file).toString, 24)}")
    }
  }

  class TestObject(c: MutableDContext) extends DObject(c, "test") {
    val a = cvar("a", 1)
    val b = cvar("b", 2)
    val x = cvar("x", 3)

    val sum = make("sum", (a, b)) { case (a, b) => Future { a + b } }
    val dec = make("dec", (sum, x)) { case (sum, x) => Future { sum - x } }

    override def toString = {
      f"{a:${Await.result(a.get, 5 seconds)}, " +
        f"b:${Await.result(b.get, 5 seconds)}, " +
        f"x:${Await.result(x.get, 5 seconds)}, " +
        f"sum:${Await.result(sum.get, 5 seconds)}, " +
        f"dec:${Await.result(dec.get, 5 seconds)}}"
    }
  }

  class FsTestObject(c: MutableDContext, dir: File) extends FsDObject(c, "persistent", dir) {
    val a = persistentVar("a", 4)
    val b = persistentVar("b", 2)
    val x = persistentVar("x", 3)

    val div = makePersistent("div", (a, b)) { case (a, b) => Future { a / b } }
    val dec = makePersistent("dec", (div, x)) { case (div, x) => Future { div - x } }

    val double = makeWrittenFile("file.bin", dec) { case (dec, f) =>
      Future { IoUtil.atomicWrite(f, 2 * dec) }
    }

    def doubleContent = double.get.map { f =>
      IoUtil.read[Int](f)
    }

    override def toString = {
      f"{a:${Await.result(a.get, 5 seconds)}, " +
        f"b:${Await.result(b.get, 5 seconds)}, " +
        f"x:${Await.result(x.get, 5 seconds)}, " +
        f"sum:${Await.result(div.get, 5 seconds)}, " +
        f"dec:${Await.result(dec.get, 5 seconds)}, " +
        f"double:${Await.result(doubleContent, 5 seconds)}}"

    }
  }

  test("basics")(Tests.inTestContext { (t, c) => {
    tContext(t, c)

    val o = new TestObject(c)
    t.tln("created test object")
    t.tln
    t.tln("object is " + o)
    t.tln
    tContext(t, c)
    t.tln
    o.a.update(4)
    t.tln("changed a to 4")
    t.tln
    t.tln("object is " + o)
    t.tln
    tContext(t, c)
    t.tln
  };
  {
    val o = new TestObject(c)
    t.tln("restored test object")
    t.tln
    t.tln("object is " + o)
    t.tln
    tContext(t, c)
  }
  })

  test("persistence") { t =>
    val f = t.file("object");
  {
    val c = MutableDContext()
    tContext(t, c);

    {
      val o = new FsTestObject(c, f)
      t.tln("created test object")
      t.tln
      t.tln("object is " + o)
      t.tln
      tContext(t, c)
      t.tln
      o.a.update(4)
      t.tln("changed a to 4")
      t.tln
      t.tln("object is " + o)
      t.tln
      tContext(t, c)
      t.tln
    };
    {
      val o = new FsTestObject(c, f)
      t.tln
      t.tln("restored test object from context")
      t.tln
      t.tln("object is " + o)
      t.tln
      tContext(t, c)
    }
  };
  {
    val c = MutableDContext()
    t.tln
    t.tln("closed the context and created new one")
    t.tln
    tDir(t, f)
    t.tln
    tContext(t, c)
    t.tln

    val o = new FsTestObject(c, f)
    t.tln("restored test object from fs")
    t.tln
    t.tln("object is " + o)
    t.tln
    tContext(t, c)

    t.tln
    t.tln("let's corrupt things, by setting b=0")
    o.b.update(0)
    t.tln
    t.tln("div try is " + Await.ready(o.div.get, 5 seconds).value.get)
    t.tln("dec try is " + Await.ready(o.dec.get, 5 seconds).value.get)
    t.tln("double try is " + Await.ready(o.doubleContent, 5 seconds).value.get)
    t.tln
    tContext(t, c)
  }
  {
    val c = MutableDContext()
    t.tln
    t.tln("closed the context and created new one")
    t.tln
    tDir(t, f)
    t.tln
    tContext(t, c)
    t.tln

    val o = new FsTestObject(c, f)
    t.tln("restored test object from fs")
    t.tln
    t.tln("div try is " + Await.ready(o.div.get, 5 seconds).value.get)
    t.tln("dec try is " + Await.ready(o.dec.get, 5 seconds).value.get)
    t.tln("double try is " + Await.ready(o.doubleContent, 5 seconds).value.get)
    t.tln
    tContext(t, c)
  }
  }

  class BarrierTestObject(c: MutableDContext, t: String => Unit)
    extends DObject(c, "barrier") {
    val a = cvar("a", 4)
    val b = cvar("b", 2)

    val slowA = make("slowA", a) { case a => Future {
      t("doing A...")
      Thread.sleep(100) // sleep 100 ms
      t("A done. ")
      a
    }}
    val slowB = make("slowB", b) { case b => Future {
      t("doing B...")
      Thread.sleep(100) // sleep 100 ms
      t("B done. ")
      b
    }}

    val div = make("div", (slowA, slowB)) { case (a, b) => Future { a / b } }

    override def toString = {
      f"{a:${Await.result(a.get, 5 seconds)}, " +
        f"b:${Await.result(b.get, 5 seconds)}, " +
        f"slowA:${Await.result(slowA.get, 5 seconds)}, " +
        f"slowB:${Await.result(slowB.get, 5 seconds)}, " +
        f"sum:${Await.result(div.get, 5 seconds)}}"

    }
  }

  test("makeBarrier") { t =>
    val c = MutableDContext()
    tContext(t, c);

    {
      val o = new BarrierTestObject(c, x => t.tln("  " + x))

      t.tln("created test object")
      t.tln
      t.tln("making multiple requests to slow a.")
      t.tln
      t.tln("a log:")

      val a1 = o.slowA.get
      val a2 = o.slowA.get
      val a3 = o.slowA.get

      val as = Await.result(a1 zip a2 zip a3, 5 seconds)

      t.tln
      t.tln(f"results are: ${as}")
      t.tln

      t.tln("making multiple requests to slow b.")
      t.tln
      t.tln("b log:")

      val b1 = o.slowB.get
      val b2 = o.slowB.get
      val b3 = o.slowB.get
      val bs = Await.result(b1 zip b2 zip b3, 5 seconds)
      t.tln
      t.tln(f"results are: ${bs}")
      t.tln
      tContext(t, c)
    }
    {
      var buf = new StringBuffer()
      val o = new BarrierTestObject(c, x => buf.append("  " + x + "\n"))
      t.tln
      t.tln("restored test object from context")
      t.tln
      t.tln("changed a to 4")
      o.a.update(4)
      t.tln
      t.tln("making multiple requests to div.")
      t.tln
      val d1 = o.div.get
      val d2 = o.div.get
      val d3 = o.div.get
      val ds = Await.result(d1 zip d2 zip d3, 5 seconds)
      t.tln("logs:")
      t.iln(buf.toString)
      buf = new StringBuffer()
      t.tln(f"results are: ${ds}")
      t.tln
      t.tln("changed a to 8")
      o.a.update(8)
      t.tln;
      {
        t.tln("making multiple requests to div.")
        t.tln
        val d1 = o.div.get
        val d2 = o.div.get
        val d3 = o.div.get
        val ds = Await.result(d1 zip d2 zip d3, 5 seconds)
        t.tln("logs:")
        t.iln(buf.toString)
        buf = new StringBuffer()
        t.tln(f"results are: ${ds}")
      }
      t.tln
      t.tln("object is " + o)
      t.tln
      tContext(t, c)
    }

  }
}