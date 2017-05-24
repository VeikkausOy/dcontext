package fi.veikkaus.dcontext

import java.io.{Closeable, File}

import com.futurice.testtoys.{TestSuite, TestTool}
import fi.veikkaus.dcontext.dobject.{DObject, FsDObject}
import fi.veikkaus.dcontext.store.IoUtil
import fi.veikkaus.dcontext.value.{DefaultReferenceManagement, RefCountManagement, RefCounted, TupleReferenceManagement}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Awaitable, Future}
import scala.concurrent.duration._

/**
  * Created by arau on 2.11.2016.
  */
class DObjectTest extends TestSuite("dobject") {

  import fi.veikkaus.dcontext.value.ReferenceManagement.implicits

  def waitString[T](t:Awaitable[T]): String = {
    try { Await.result(t, 5 seconds).toString } catch { case e : Exception => "!'" + e.toString + "'"}
  }

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

    val sum = make("sum", a zip b) { case (a, b) => Future {
      a + b
    }
    }
    val dec = make("dec", sum zip x) { case (sum, x) => Future {
      sum - x
    }
    }

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

    val div = makePersistent("div", a zip b) { case (a, b) =>
      Future {
        a / b
      }
    }
    val dec = makePersistent("dec", div zip x) { case (div, x) => Future {
      div - x
    }
    }

    val double = makeWrittenFile("file.bin", dec) { case (dec, f) =>
      Future {
        IoUtil.atomicWrite(f, 2 * dec)
      }
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
    }
    }
    val slowB = make("slowB", b) { case b => Future {
      t("doing B...")
      Thread.sleep(100) // sleep 100 ms
      t("B done. ")
      b
    }
    }

    val div = make("div", slowA zip slowB) { case (a, b) => Future {
      a / b
    }
    }

    override def toString = {
      f"{a:${waitString(a.get)}, " +
        f"b:${waitString(b.get)}, " +
        f"slowA:${waitString(slowA.get)}, " +
        f"slowB:${waitString(slowB.get)}, " +
        f"div:${waitString(div.get)}}"

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

  case class FragileException(e: String) extends RuntimeException(e)

  class FailingTestObject(c: MutableDContext, t: String => Unit)
    extends DObject(c, "barrier") {

    var failing = true

    val a = cvar("a", 4)
    val b = cvar("b", 2)

    def filter(e: Throwable) = e match {
      case FragileException(_) => false
      case _ => true
    }

    val failingA = make("failingA", a, Some(filter)) { case a => Future {
      t("doing failingA..")
      if (failing) {
        t("failingA failed.")
        throw FragileException("A failed!")
      }
      t("failingA done.")
      a
    }
    }
    val failingB = make("failingB", b, Some(filter)) { case b => Future {
      t("doing failingB..")
      if (failing) {
        t("failingB failed.")
        throw FragileException("B failed!")
      }
      t("failingB done.")
      b
    }
    }

    val div = make("div", failingA zip failingB) { case (a, b) => Future {
      a / b
    }
    }

    override def toString = {
      f"{a:${Await.result(a.get, 5 seconds)}, " +
        f"b:${Await.result(b.get, 5 seconds)}, " +
        f"failingA:${Await.result(failingA.get, 5 seconds)}, " +
        f"failingB:${Await.result(failingB.get, 5 seconds)}, " +
        f"div:${Await.result(div.get, 5 seconds)}}"

    }
  }


  test("failing") { t =>
    val c = MutableDContext()
    tContext(t, c);

  {
    var buf = new StringBuffer()
    val o = new FailingTestObject(c, x => buf.append("  " + x + "\n"))

    t.tln("created test object")
    t.tln
    t.tln("making multiple requests to failing a.")
    t.tln
    t.tln(f"results is: ${Await.result(o.failingA.getTry, 5 seconds)}")
    t.tln
    t.tln("logs:")
    t.iln(buf.toString)
    buf = new StringBuffer()
    t.tln
    t.tln(f"results is: ${Await.result(o.failingA.getTry, 5 seconds)}")
    t.tln
    t.tln("logs:")
    t.iln(buf.toString)
    buf = new StringBuffer()
    t.tln
    t.tln(f"results is: ${Await.result(o.failingA.getTry, 5 seconds)}")
    t.tln
    t.tln("logs:")
    t.iln(buf.toString)
    buf = new StringBuffer()
    t.tln

    t.tln("making things succeed..")
    o.failing = false
    t.tln
    t.tln(f"results is: ${Await.result(o.failingA.getTry, 5 seconds)}")
    t.tln
    t.tln("logs:")
    t.iln(buf.toString)
    buf = new StringBuffer()
    t.tln
    t.tln(f"results is: ${Await.result(o.failingA.getTry, 5 seconds)}")
    t.tln
    t.tln("logs:")
    t.iln(buf.toString)
    buf = new StringBuffer()
    t.tln
  }
  }

  class CloseableRegistry {
    val values = new ArrayBuffer[Closeable]()
    def add(v:Closeable): Unit = synchronized {
       values += v
    }
    def remove(v:Closeable) : Unit = synchronized {
      values.remove(values.indexOf(v))
    }
  }

  class CloseableValue[T](name:String, v:T, registry:CloseableRegistry) extends Closeable {
    registry.add(this)
    private var value : Option[T] = Some(v)
    def get = value.getOrElse(throw new RuntimeException(name + " was closed"))
    def update(v:T) = {
      value = Some(v)
    }
    def close = {
      registry.remove(this)
      value = None
    }
    override def toString = value.map(_.toString).getOrElse("(closed)")
  }

  def waitStringAndClose[T](t:Awaitable[RefCounted[CloseableValue[T]]]): String = {
    try {
      val openedRef = Await.result(t, 5 seconds)
      try {
        openedRef.toString
      } finally {
        openedRef.close
      }
    } catch { case e : Exception => "!'" + e.toString + "'"}
  }

  class RefTestObject(c: MutableDContext, t:TestTool, registry:CloseableRegistry) extends DObject(c, "ref-test") {
    val a = cvar("a", 1)
    val b = cvar("b", 2)
    val x = cvar("x", 3)
    val sleepMs = cvar("sleepMs", 0)

    val sum = make("sum", a zip b) { case (a, b) => Future {
        RefCounted(new CloseableValue[Int]("sum", a + b, registry), 1)
      }
    }(new RefCountManagement[CloseableValue[Int]], new DefaultReferenceManagement[(Int, Int)])
    val dec = make("dec", sum zip (x zip sleepMs)) { case (sum, (x, sleepMs)) => Future {
        t.tln("@RefTestObject.dec:   started dec")
        Thread.sleep(sleepMs) // give us time to close the sum
        t.tln("@RefTestObject.dec: slept, returning value")
        RefCounted(new CloseableValue[Int]("dec", sum.value.get - x, registry), 1)
      }
    }(new RefCountManagement[CloseableValue[Int]],
      new TupleReferenceManagement[RefCounted[CloseableValue[Int]], (Int, Int)]()(
        new RefCountManagement[CloseableValue[Int]],
        new TupleReferenceManagement[Int, Int]() // use implicits management
      ))

    override def toString = {
      f"{a:${waitString(a.get)}, " +
        f"b:${waitString(b.get)}, " +
        f"x:${waitString(x.get)}, " +
        f"sum:${waitStringAndClose(sum.get)}, " +
        f"dec:${waitStringAndClose(dec.get)}}"
    }
  }


  test("refs") { t =>
    val registry = new CloseableRegistry()
    def tRegistry() : Unit = {
      t.tln
      t.tln(registry.values.size + " objects are open:")
      registry.values.foreach { v =>
        t.tln("  " + v)
      }
      t.tln("  (end of list)")
      t.tln
    }
    t.tln("started test.")
    tRegistry();
    {
      val c = MutableDContext()
      tContext(t, c);

      val o = new RefTestObject(c, t, registry)

      t.tln("created test object.")
      t.tln
      t.tln("object is " + o)
      t.tln

      tContext(t, c)
      tRegistry()

      o.a.update(4)
      t.tln("changed a to 4")
      t.tln
      t.tln("object is " + o)
      t.tln
      tContext(t, c)
      tRegistry()

      t.tln("let's do the test, where we break things")

      ;
      {
        t.tln("let's first turn on sleep...")
        o.sleepMs.update(250)
        t.tln("sleep is now " + o.sleepMs() + "ms")
        t.tln("let's then request dec...")
        val decResult = o.dec.get
        Thread.sleep(100)
        t.tln("...and close the sum, while dec is sleeping!")
        o.sum.valueStore.update(None)
        t.tln
        t.tln("ref-test.sum is now: " + c.get[Any]("ref-test.sum"))
        tRegistry()
        t.tln("let's wait for the result..")
        t.tln("dec is " + waitStringAndClose(decResult))
        t.tln
        t.tln("object is " + o)
      }
      t.tln("let's check situation, where we have several requests")
      t.tln

      t.tln("let's update source data by settin a to 7")
      o.a.update(7)

      t.tln("let's request dec 3 times...")
      val decResult1 = o.dec.get
      val decResult2 = o.dec.get
      val decResult3 = o.dec.get
      Thread.sleep(100)
      t.tln
      t.tln("dec request 1 resulted in " + waitStringAndClose(decResult1))
      t.tln("dec request 2 resulted in " + waitStringAndClose(decResult2))
      t.tln("dec request 3 resulted in " + waitStringAndClose(decResult3))
      t.tln
      t.tln("object is " + o)
      c.close
    }
    t.tln("closed the context and the test object.")
    tRegistry()
  }
}