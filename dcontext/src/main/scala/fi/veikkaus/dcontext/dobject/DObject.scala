package fi.veikkaus.dcontext.dobject

import java.io.{Closeable, File, IOException}
import java.nio.file.Files.move
import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, REPLACE_EXISTING}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Paths, Path, SimpleFileVisitor, FileVisitResult}
import java.util.concurrent.{TimeUnit, TimeoutException}

import scala.concurrent.ExecutionContext.Implicits.global
import fi.veikkaus.dcontext._
import fi.veikkaus.dcontext.store._
import fi.veikkaus.dcontext.value._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


/**
  * Created by arau on 1.11.2016.
  */
class DObject(val c:MutableDContext, val dname:String) extends Closeable {

  private val logger = LoggerFactory.getLogger(getClass)

  private var closeables = ArrayBuffer[Closeable]()

  def bind[T <: Closeable](closeable:T): T = {
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
    bind (new Closeable {
      def close = {
        val res = rv.completed
        while (!res.isCompleted) {
          try {
            Await.result(res, Duration(30, TimeUnit.SECONDS))
          } catch {
            case e : TimeoutException =>
              logger.error("make failed to complete within 30 second, still waiting", e)
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
        move(tmp.toPath, file.toPath, ATOMIC_MOVE, REPLACE_EXISTING)
        file
      }
    }(new DefaultReferenceManagement[File], sourceRefs)
  }

  def deleteRecursively(path: Path) : Unit = {
    Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }

      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }
    })
  }

  private def moveFileAtomically(from: Path, to: Path): Path = {
    val movedPath = move(from, to, ATOMIC_MOVE, REPLACE_EXISTING)
    if (Files.exists(movedPath)) {
      movedPath
    } else {
      throw new RuntimeException(s"File move '${from}' -> '${to}' failed. Result file does not exist after move")
    }
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
      val tmp = Paths.get(dir.getParent, s"${dir.getName}.tmp")
      deleteRecursively(tmp)
      Files.createDirectories(tmp.getParent)
      writeFile(s, tmp.toFile).map { v =>
        val del = Paths.get(dir.getParent, s"${dir.getName}.del")
        if (Files.exists(del)) {
          deleteRecursively(del)
        }
        // unsafe non-atomic switch. Try to make it safer by using 'global lock'
        c synchronized {
          moveFileAtomically(dir.toPath, del)
          moveFileAtomically(tmp, dir.toPath)
        }
        deleteRecursively(del)
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
