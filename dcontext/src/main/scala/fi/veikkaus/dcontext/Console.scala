package fi.veikkaus.dcontext

/**
  * Created by arau on 24.5.2016.
  */
import java.io.{File, PrintWriter}
import java.util.regex.Pattern

import scala.collection.JavaConversions._
import jline.console.ConsoleReader
import jline.console.completer.{Completer, StringsCompleter}
import jline.console.history.FileHistory

/**
  * Created by arau on 24.5.2016.
  */
class Console(var staticLayer : DContext = DContext.empty) extends DSystem {

  val classLoader = new DynamicClassLoader()

  val reader = new ConsoleReader()
  var contextCompleter: Option[Completer] = None
  val out = new PrintWriter(reader.getOutput)

  val history = new FileHistory(new File(".dcontext_history").getAbsoluteFile)
  reader.setHistory(history)

  val quit = new Object // unique key

  val tasks = Seq[(String, HelpfulContextTask)](
    ("-h", dtask("this help",            (c, args) => printHelp)),
    ("--reload", dtask("reload classes",  (c, args) => reload)),
    ("-q", dtask("quits console",        (c, args) => quit)),
    ("-p", dtask("adds classpath",       (c, args) => args.foreach(addClassPaths(_)))),
    ("-m", dtask("mounts context",       (c, args) => args.foreach(mount(_)))),
    ("-l", dtask("lists context values", list)),
    ("-i", dtask("interactive console",  (c, args) => console)),
    ("-r", dtask("remove data (e.g. '-r fooX' or '-r .*')", (c, args) => remove(args))),
    ("-x", dtask("execute tasks matching regex (e.g. '-x test.*'", (c, args) => execAll(args))),
    ("-d", dtask("display class loader info",    (c, args) => classLoaderInfo)),
    ("--uris", dtask("display resource URIs",    (c, args) => resourceURIs(args)))
  )

  val systemLayer =
    DContext(
      tasks.map(e => (e._1, e._2)).toMap
      ++ Map(DContext.systemId -> Console.this))

  val dataLayer = MutableDContext.apply

  def context =
    (systemLayer ++ staticLayer) ++ dataLayer

  def addClassPaths(classPaths:String) = {
    val files = classPaths.split(":").map(new File(_))
    classLoader.addLoader(files)
 //   files.map(classLoader.loadClassesRecursively(_))
  }

  def mount(str:String): Unit = {
    val c = classLoader.newProxyInstance[DContext](classOf[DContext], str, Array(), Array())
    staticLayer ++= c
  }
  def console = {
    reader.setPrompt("$ ")

    var line: String = null
    out.println("type -h for instructions:")
    while ( { line = reader.readLine(); line != null && line != "-q"} ) {
      val l = line.trim()
      if (l != "") {
        process(Array(l))
      }
    }
  }

  def printHelp {
    tasks.foreach{ e => out.println(f"${e._1}%-30s ${e._2.help()}") }
  }

  def reload : Unit = {
    classLoader.reloadAll()
  }

  def classLoaderInfo: Unit = {
    val now = System.currentTimeMillis()
    classLoader.loadedClasses.foreach { e =>
      out.println(f"${e._1}%-60s ${(now-e._2.lastModified)/1000}s old")
    }
  }

  def list(c:MutableDContext, args:Array[String]) {
    val c = (staticLayer ++ dataLayer)
    def list(filter:String => Boolean) = {
      c.keySet.toArray.sorted.filter(filter).foreach { key =>
        val v = c.getType(key).get
        if (v.isAssignableFrom(classOf[HelpfulContextTask])) {
          out.println(f"${key}%-30s ${c(key).asInstanceOf[HelpfulContextTask].help()}")
        } else {
          out.println(f"${key}%-30s ${v.getName}")
        }
      }
    }
    args match {
      case Array() => list(_ => true)
      case v => v.foreach { arg =>
        val p = Pattern.compile(arg)
        list(v => p.matcher(v).matches())
      }
    }
  }
  def remove(args:Array[String]) = {
    args.flatMap { arg =>
      val p = Pattern.compile(arg)
      dataLayer.keySet.filter(p.matcher(_).matches)
    }.map {
      dataLayer.remove(_)
    }
  }
  def resourceURIs(args:Array[String]) = {
    args.foreach { arg =>
      out.append(arg + ":\n")
      classLoader.getResources(arg).foreach { url =>
        out.append("  " + url.toString + "\n")
      }
    }
  }

  def dtask(h:String, task:(MutableDContext, Array[String])=>Any) =
    new HelpfulContextTask() {
      def run(context: MutableDContext, args: Array[String]) = {
        task(context, args)
      }
      def help = h
    }

  def close() : Unit = {
    history.flush()
    dataLayer.close
  }

  def updateContextCompleter(): Unit = {
    val newCompleter = new StringsCompleter(context.keySet)
    contextCompleter.foreach(c => reader.removeCompleter(c))
    reader.addCompleter(newCompleter)
  }

  def exec(parts:Array[String]): Unit = {
    if (parts.size > 0) {
      val v = context.get[Any](parts.head)
      //          out.println(v)
      v match {
        case Some(task: ContextTask) if task.isInstanceOf[ContextTask] =>
          (task: ContextTask).run(context, parts.tail.toArray)
        case Some(v) =>
          out.println(v.toString)
        case None =>
          out.println("invalid argument: '" + parts.head + "'")
      }
    }
  }

  def execAll(args:Array[String]): Unit = {
    args.flatMap { arg =>
      val p = Pattern.compile(arg)
      context.keySet.filter(p.matcher(_).matches)
    }.map { task =>
      exec(Array(task))
    }
  }

  @throws[Exception]
  def process(args: Array[String]) {
    val oldCl = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(classLoader)

    updateContextCompleter()

    try {
      args.foreach { arg =>
        exec(arg.trim().split(" "))
      }
    } catch {
      case e => e.printStackTrace()
    } finally {
      Thread.currentThread().setContextClassLoader(oldCl)
    }
  }
}

object Console {
  def exec(context:DContext, args:Array[String]): Unit = {
    val console = new Console(context)
    try {
      console.exec(args)
      console.out.flush
    } finally {
      console.close
    }
  }
  def main(args: Array[String]) {
    val console = new Console

    // Close the console properly (in particular, flush the history)
    // even when the JVM is terminated.
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        console.close()
      }
    })

    try {
      try {
        console.process(args)
      } finally {
        console.close()
      }
    } catch {
      case e => e.printStackTrace()
    }
  }
}