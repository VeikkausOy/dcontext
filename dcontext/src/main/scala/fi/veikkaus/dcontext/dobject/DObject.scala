package fi.veikkaus.dcontext.dobject

import java.io.{Closeable, File}

import scala.concurrent.ExecutionContext.Implicits.global
import fi.veikkaus.dcontext.MutableDContext
import fi.veikkaus.dcontext.store._
import fi.veikkaus.dcontext.value._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scalax.file.Path


/**
  * Created by arau on 1.11.2016.
  */
class DObject(val c:MutableDContext, val dname:String) extends Closeable {

  private var closeables = ArrayBuffer[Closeable]()

  private def bind[T <: Closeable](closeable:T): T = {
    closeables += closeable
    closeable
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
    closeables.foreach { _.close() }
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
  def make[Type, Source, Version](n:String,
                                  source:Versioned[Source, Version],
                                  throwableFilter:Option[Throwable => Boolean] = None)
                                 (f:Source=>Future[Type]) = {
    Make(maybeFiltered(contextStore[Try[Type]](n), throwableFilter),
         contextStore[Version](f"$n.version"))(source)(f)
  }
  def makeAutoClosed[Type, Source, Version](n:String,
                                            source:Versioned[Source, Version],
                                            throwableFilter:Option[Throwable => Boolean] = None)(f:Source=>Future[Type])(closer:Type => Unit) = {
    Make(maybeFiltered(contextTryStore[Type](n, Some(closer)), throwableFilter),
         contextStore[Version](f"$n.version"))(source)(f)
  }

  def makeHeap[Type, Source, Version](source:Versioned[Source, Version],
                                      throwableFilter:Option[Throwable => Boolean] = None)(f:Source=>Future[Type]) = {
    Make(maybeFiltered(heapTryStore[Type](), throwableFilter),
         HeapStore[Version]())(source)(f)
  }
  def makeHeapAutoClosed[Type, Source, Version](source:Versioned[Source, Version],
                                                throwableFilter:Option[Throwable => Boolean] = None)(f:Source=>Future[Type])(closer:Type => Unit) = {
    Make(maybeFiltered(heapTryStore[Type](Some(closer)), throwableFilter),
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

  def fileStore[T <: AnyRef](n:String) = {
    val file = allocFile(n)
    CachedStore[T](WeakStore[T](), FileStore(file))
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

  def persistentVar[T](n:String, init : => T) = {
    new VersionedVar[T](new StoreVar[T](contextAndFileStore[T](n), init),
                        new StoreVar[Long](contextAndFileStore[Long](n + ".version"), 0))
  }

  /* stored mainly in filesystem, lifted to memory only as needed*/
  def makeFile[Type <: AnyRef, Source, Version]
  (n:String, source:Versioned[Source, Version], throwableFilter:Option[Throwable => Boolean] = None)
  (f:Source=>Future[Type]) = {
    Make(
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
  (writeFile:(Source, File)=>Future[Unit]) = {
    val file = allocFile(n)
    Make(maybeFiltered(new FileNameTryStore(file), throwableFilter),
         contextAndFileStore[Version](f"$n.version"))(source) { s =>
      val tmp = new File(file.getParent, file.getName + ".tmp")
      tmp.delete()
      writeFile(s, tmp).map { v =>
        tmp.renameTo(file); // atomic replace
        file
      }
    }
  }

  def makeWrittenDir[Type <: AnyRef, Source, Version]
  (n:String,
   source:Versioned[Source, Version],
   throwableFilter:Option[Throwable => Boolean] = None)
  (writeFile:(Source, File)=>Future[Unit]) = {
    val dir = allocFile(n)
    Make(maybeFiltered(new FileNameTryStore(dir), throwableFilter),
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
    }
  }

  def makePersistent[Type, Source, Version]
    (n:String,
     source:Versioned[Source, Version],
     throwableFilter:Option[Throwable => Boolean] = None)
    (f:Source=>Future[Type]) = {
    Make(maybeFiltered(contextAndFileTryStore[Type](n), throwableFilter),
         contextAndFileStore[Version](f"$n.version"))(source)(f)
  }

}
