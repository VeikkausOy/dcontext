package fi.veikkaus.dcontext.dobject

import java.io.{Closeable, File, StringWriter}
import java.util.concurrent.{CancellationException, TimeUnit, TimeoutException}

import scala.concurrent.ExecutionContext.Implicits.global
import fi.veikkaus.dcontext.MutableDContext
import fi.veikkaus.dcontext.store._
import fi.veikkaus.dcontext.value._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scalax.file.Path


/**
  * Created by arau on 1.11.2016.
  */
class DObject(val c:MutableDContext, val dname:String) extends Closeable {

  private val logger = LoggerFactory.getLogger(getClass)

  private var closeFirst = ArrayBuffer[Closeable]()
  private var closeables = ArrayBuffer[Closeable]()

  var isCancelled = false

  def job[T](ex: ExecutionContext, jobName:String)(job : (()=>Unit)=>T): Future[T] = {
    Future[T] {
      try {
        def checkCancels() = {
          if (isCancelled) {
            logger.warn(dname + " job " + jobName + " was cancelled")
            throw new CancellationException()
          }
        }

        checkCancels
        logger.info("started job " + jobName)
        val rv = job(checkCancels)
        checkCancels
        logger.info("job " + jobName + " done")
        rv
      } catch {
        case e : CancellationException =>
          throw e //
        case e : Exception =>
          logger.error(dname + " job " + jobName + " failed with " + e, e)
          throw e
      }
    }(ex)
  }
  def job[T](ex: ExecutionContext)(j : (()=>Unit)=>T): Future[T] = {
    job[T](ex, j.toString)(j)
  }

  def bindPriority[T <: Closeable](closeable:T): T = {
    closeFirst += closeable
    closeable
  }
  def bind[T <: Closeable](closeable:T): T = {
    closeables += closeable
    closeable
  }

  /** this should mark all active jobs e.g. in makes to stop */
  def cancel = {
    isCancelled = true
    closeFirst.foreach { _.close }
  }

  val names = mutable.HashSet[String]()

  def allocName(n:String) = {
    val fullName = dname + "." + n
    names += fullName
    fullName
  }

  /**
    * NOTE: this frees and closes only the heap resources.
    * Dcontext and file system are left intact.
    */
  def close() = {
    closeFirst.foreach { c =>
      try {
        c.close
      } catch {
        case e : Exception =>
          logger.error("closing " + c + " failed.", e)
      }
    }
    closeables.foreach { c =>
      try {
        c.close
      } catch {
        case e : Exception =>
          logger.error("closing " + c + " failed.", e)
      }
     }
    closeables.clear
  }

  /**
    * NOTE: this frees both heap and it releases the dcontext resources
    * Filesystem resources are left intact.
    */
  def remove = {
    close
    names.foreach ( c.remove(_) )
  }

  def heapTryStore[T](closer:Option[T => Unit] = None) = {
    closer match {
      case Some(c) => bind(new AutoClosedTryStore[T] (new HeapStore[Try[T]](), c))
      case None => HeapStore[Try[T]]()
    }
  }
  //  lazy val lock = contextVar(allocName(".lock"), new Object)

  def contextTryStore[T](n:String, closer:Option[T => Unit] = None) = {
    new ContextTryStore[T](c, allocName(n), closer)
  }

  def maybeFiltered[T](store:Store[Try[T]], filter:Option[Throwable => Boolean]) : Store[Try[T]] = {
    filter match {
      case Some(f) =>
        new FilteredTryStore[T](store, _ match {
          case Success(v) => true
          case Failure(err) => f(err)
        })
      case None =>
        store
    }
  }

  def contextStore[T](n:String, closer:Option[T => Unit] = None) = {
    new ContextStore[T](c, allocName(n), closer)
  }
  def contextVar[T](n:String, init : => T, closer : Option[T => Unit] = None) = {
    new VersionedVar[T](new StoreVar[T](contextStore[T](n, closer), init),
                        new StoreVar[Long](contextStore[Long](n + ".version"), 0))
  }
  def cvar[T](n:String, init : => T, closer : Option[T => Unit] = None) = contextVar[T](n, init, closer)
  def lazyContextVal[T](n:String, init : => Future[T]) = {
    new AsyncLazyVal[T](new ContextStore[T](c, dname + "." + n), init)
  }
  def addMake[Type, Source, Version](
    valueStore:Store[Try[Type]], versionStore:Store[Version])(
    source:Versioned[Source, Version])
  (build : Source => Future[Type])
  (implicit valueRefs : ReferenceManagement[Type], sourceRefs : ReferenceManagement[Source]) = {
    val rv = Make(valueStore, versionStore)(source)(build)(valueRefs, sourceRefs)
    bindPriority(rv)// first close makes, in order to prevent creation of new tasks
    val stackTracer = new RuntimeException("make created here")
    bind (new Closeable {
      def close = {
        val res = rv.completed
        while (!res.isCompleted) {
          val waitS = 10*60
          try {
            Await.result(res, Duration(waitS, TimeUnit.SECONDS))
          } catch {
            case e : TimeoutException =>
              logger.error(f"make failed to complete within $waitS second, still waiting", stackTracer)
          }
        }
      }
    })
    rv
  }

  def make[Type, Source, Version](n:String,
                                  source:Versioned[Source, Version],
                                  throwableFilter:Option[Throwable => Boolean] = None)
                                 (f:Source=>Future[Type])
                                 (implicit valueRefs : ReferenceManagement[Type],
                                  sourceRefs : ReferenceManagement[Source]) = {
    addMake(maybeFiltered(contextTryStore[Type](n, Some(valueRefs.dec)), throwableFilter),
         contextStore[Version](f"$n.version"))(source)(f)
  }
  def makeAutoClosed[Type, Source, Version](n:String,
                                            source:Versioned[Source, Version],
                                            throwableFilter:Option[Throwable => Boolean] = None)
                                           (f:Source=>Future[Type])(closer:Type => Unit)
                                           (implicit valueRefs : ReferenceManagement[Type],
                                            sourceRefs : ReferenceManagement[Source]) = {
    addMake(maybeFiltered(contextTryStore[Type](n, Some(closer)), throwableFilter),
                          contextStore[Version](f"$n.version"))(source)(f)
  }

  def makeHeap[Type, Source, Version](source:Versioned[Source, Version],
                                      throwableFilter:Option[Throwable => Boolean] = None)
                                     (f:Source=>Future[Type])
                                     (implicit valueRefs : ReferenceManagement[Type],
                                      sourceRefs : ReferenceManagement[Source])= {
    addMake(maybeFiltered(heapTryStore[Type](Some(valueRefs.dec)), throwableFilter),
         HeapStore[Version]())(source)(f)
  }
  def makeHeapAutoClosed[Type, Source, Version](source:Versioned[Source, Version],
                                                throwableFilter:Option[Throwable => Boolean] = None)
                                               (f:Source=>Future[Type])
                                               (closer:Type => Unit)
                                               (implicit valueRefs : ReferenceManagement[Type],
                                                sourceRefs : ReferenceManagement[Source])= {
    addMake(maybeFiltered(heapTryStore[Type](Some(closer)), throwableFilter),
         HeapStore[Version]())(source)(f)
  }
}

class FsDObject(c:MutableDContext, name:String, val dir:File) extends DObject(c, name) {

  val files = mutable.HashSet[File]()

  def allocFile(n:String) = {
    dir.mkdirs()
    val file = new File(dir, n)
    synchronized {
      files += file
    }
    file
  }
  /**
    * NOTE: this frees heap resources, dcontext resources, and fs resources.
    */
  def delete : Unit = {
    remove
    synchronized {
      files.foreach(_.delete)
      dir.list().size == 0 && dir.delete()
    }
  }

  def rawFileStore[T](n:String) = {
    val file = allocFile(n)
    FileStore[T](file)
  }

  def fileStore[T <: AnyRef](n:String) = {
    CachedStore[T](WeakStore[T](), rawFileStore[T](n))
  }
  def fileTryStore[T <: AnyRef](n:String) = {
    val file = allocFile(n)
    CachedStore[Try[T]](WeakTryStore[T](), FileTryStore(file))
  }
  def contextAndFileTryStore[T](n:String) = {
    val name = allocName(n)
    val file = allocFile(n)
    CachedStore[Try[T]](ContextStore(c, name),
                        FileTryStore(file))
  }
  def contextAndFileStore[T](n:String) = {
    val name = allocName(n)
    val file = allocFile(n)
    CachedStore[T](ContextStore(c, name),
                   FileStore(file))
  }

  def fileVar[T <: AnyRef](n:String, init : => T) = {
    new VersionedVar[T](
      new StoreVar[T](fileStore[T](n), init),
      new StoreVar[Long](contextAndFileStore[Long](n + ".version"), 0))
  }

  def persistentVar[T](n:String, init : => T) = {
    new VersionedVar[T](new StoreVar[T](contextAndFileStore[T](n), init),
                        new StoreVar[Long](contextAndFileStore[Long](n + ".version"), 0))
  }

  /**
    * TODO: add autoclosing using reference management
    */
  def makeFile[Type <: AnyRef, Source, Version]
  (n:String, source:Versioned[Source, Version], throwableFilter:Option[Throwable => Boolean] = None)
  (f:Source=>Future[Type])
  (implicit valueRefs : ReferenceManagement[Type],
   sourceRefs : ReferenceManagement[Source])= {
    addMake(
      maybeFiltered(fileTryStore[Type](n), throwableFilter),
      contextAndFileStore[Version](f"$n.version"))(source)(f)
  }

  /**
    * NOTE: the file given as an argument to writeFile() is a temporary file,
    *       that is atomically renamed to the actual file, once ready
    */
  def makeWrittenFile[Type <: AnyRef, Source, Version]
  (n:String,
   source:Versioned[Source, Version],
   throwableFilter:Option[Throwable => Boolean] = None)
  (writeFile:(Source, File)=>Future[Unit])
  (implicit sourceRefs : ReferenceManagement[Source])= {
    val file = allocFile(n)
    addMake(maybeFiltered(new FileNameTryStore(file), throwableFilter),
         contextAndFileStore[Version](f"$n.version"))(source) { s =>
      val tmp = new File(file.getParent, file.getName + ".tmp")
      tmp.delete()
      writeFile(s, tmp).map { v =>
        tmp.renameTo(file); // atomic replace
        file
      }
    }(new DefaultReferenceManagement[File], sourceRefs)
  }

  /**
    * NOTE: There is a race condition, where abrubt shutdown make erase the
    *       make's stored state, when make is storing the new state.
    */
  def makeWrittenDir[Type <: AnyRef, Source, Version]
  (n:String,
   source:Versioned[Source, Version],
   throwableFilter:Option[Throwable => Boolean] = None)
  (writeFile:(Source, File)=>Future[Unit])
  (implicit sourceRefs : ReferenceManagement[Source]) = {
    val dir = allocFile(n)
    addMake(maybeFiltered(new FileNameTryStore(dir), throwableFilter),
      contextAndFileStore[Version](f"$n.version"))(source) { s =>
      val tmp = new File(dir.getParent, dir.getName + ".tmp")
      Path(tmp).deleteRecursively()
      tmp.mkdirs()
      writeFile(s, tmp).map { v =>
        val del = new File(dir.getParent, dir.getName + ".del")
        if (del.exists()) Path(del).deleteRecursively()
        // unsafe non-atomic switch. Try to make it safer by using 'global lock'
        c synchronized {
          dir.renameTo(del)
          tmp.renameTo(dir)
        }
        Path(del).deleteRecursively()

        //
        dir
      }
    }(new DefaultReferenceManagement[File], sourceRefs)
  }

  /**
    * TODO: add autoclosing using reference management
    */
  def makePersistent[Type, Source, Version]
    (n:String,
     source:Versioned[Source, Version],
     throwableFilter:Option[Throwable => Boolean] = None)
    (f:Source=>Future[Type])
    (implicit valueRefs : ReferenceManagement[Type],
     sourceRefs : ReferenceManagement[Source]) = {
    addMake(maybeFiltered(contextAndFileTryStore[Type](n), throwableFilter),
         contextAndFileStore[Version](f"$n.version"))(source)(f)
  }

}
