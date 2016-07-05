package fi.veikkaus.dcontext

/**
  * Created by arau on 24.5.2016.
  */
import java.io.{File, PrintWriter}
import java.util.regex.Pattern

import scala.collection.JavaConversions._

import jline.console.ConsoleReader
import jline.console.completer.{Completer, StringsCompleter}

/**
  * Created by arau on 24.5.2016.
  */
class Console(var staticLayer : DContext = DContext.empty) extends DSystem {

  val classLoader = new DynamicClassLoader()

  val reader = new ConsoleReader()
  var contextCompleter: Option[Completer] = None
  val out = new PrintWriter(reader.getOutput)

  val quit = new Object // unique key

  val tasks = Seq[(String, HelpfulContextTask)](
    ("-h", dtask("this help",            (c, args) => printHelp)),
    ("--reload", dtask("reload classes",  (c, args) => reload)),
    ("-q", dtask("quits console",        (c, args) => quit)),
    ("-p", dtask("adds classpath",       (c, args) => args.foreach(addClassPaths(_)))),
    ("-m", dtask("mounts context",       (c, args) => args.foreach(mount(_)))),
    ("-l", dtask("lists context values", list)),
    ("-i", dtask("interactive console",  (c, args) => console)),
    ("-r", dtask("remove data (e.g. '-r fooX' or '-r .*')",          (c, args) => remove(args))),
    ("-d", dtask("display class loader infor", (c, args) => classLoaderInfo))
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
    val c = classLoader.newProxyInstance[DContext](classOf[DContext], str)
    staticLayer ++= c
  }
  def console = {
    reader.setPrompt("$ ")

    var line: String = null
    out.println("type -h for instructions:")
    while ( { line = reader.readLine().trim; line != null && line != "-q"} ) {
      if (line != "") {
        process(Array(line))
      }
    }
  }

  def printHelp {
    tasks.foreach{ e => out.println(f"${e._1}%-24s ${e._2.help()}") }
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
    c.keySet.toArray.sorted.foreach { key =>
      val v = c.getType(key).get
      if (v.isAssignableFrom(classOf[HelpfulContextTask])) {
        out.println(f"${key}%-24s ${c(key).asInstanceOf[HelpfulContextTask].help()}")
      } else {
        out.println(f"${key}%-24s ${v.getName}")
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

  def dtask(h:String, task:(MutableDContext, Array[String])=>Any) =
    new HelpfulContextTask() {
      def run(context: MutableDContext, args: Array[String]) = {
        task(context, args)
      }
      def help = h
    }

  def close() : Unit = dataLayer.close

  def updateContextCompleter(): Unit = {
    val newCompleter = new StringsCompleter(context.keySet)
    contextCompleter.foreach(c => reader.removeCompleter(c))
    reader.addCompleter(newCompleter)
  }

  @throws[Exception]
  def process(args: Array[String]) {
    val oldCl = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(classLoader)

    updateContextCompleter()

    try {
      args.foreach { arg =>
        val parts = arg.trim().split(" ")
        if (parts.size > 0) {
          val v = context.get[Any](parts.head)
//          out.println(v)
          v match {
            case Some(task : ContextTask) if task.isInstanceOf[ContextTask] =>
              (task: ContextTask).run(context, parts.tail.toArray)
            case Some(v) =>
              out.println(v.toString)
            case None =>
              out.println("invalid argument: '" + parts.head + "'")
          }
        }
      }
    } catch {
      case e => e.printStackTrace()
    } finally {
      Thread.currentThread().setContextClassLoader(oldCl)
    }
  }
}

object Console {
  def main(args: Array[String]) {
    val run = new Console
    try {
      try {
        run.process(args)
      } finally {
        run.close
      }
    } catch {
      case e => e.printStackTrace()
    }
  }
}