package fi.veikkaus.dcontext.dobject

import java.io.File

import scala.concurrent.ExecutionContext.Implicits.global

import fi.veikkaus.dcontext.MutableDContext
import fi.veikkaus.dcontext.store._
import fi.veikkaus.dcontext.value._

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try


/**
  * Created by arau on 1.11.2016.
  */
class DObject(val c:MutableDContext, val dname:String) {

  val names = mutable.HashSet[String]()

  def allocName(n:String) = {
    val fullName = dname + "." + n
    names += fullName
    fullName
  }

  def remove = {
    names.foreach ( c.remove(_) )
  }

//  lazy val lock = contextVar(allocName(".lock"), new Object)

  def contextTryStore[T](n:String, closer:Option[T => Unit] = None) = {
    new ContextTryStore[T](c, allocName(n), closer)
  }
  def contextStore[T](n:String) = {
    new ContextStore[T](c, allocName(n))
  }
  def contextVar[T](n:String, init : => T) = {
    new VersionedVar[T](new StoreVar[T](contextStore[T](n), init),
                        new StoreVar[Long](contextStore[Long](n + ".version"), 0))
  }
  def cvar[T] = contextVar[T] _
  def lazyContextVal[T](n:String, init : => Future[T]) = {
    new AsyncLazyVal[T](new ContextStore[T](c, dname + "." + n), init)
  }
  def make[Type, Source, Version](n:String, source:Versioned[Source, Version])(f:Source=>Future[Type]) = {
    Make(contextStore[Try[Type]](n), contextStore[Version](f"$n.version"))(source)(f)
  }
  def make[Type, Source1, Version1, Source2, Version2]
    (n:String, sources:(Versioned[Source1, Version1], Versioned[Source2, Version2]))
    (f:((Source1, Source2))=>Future[Type]) = {
    Make(contextStore[Try[Type]](n), contextStore[(Version1, Version2)](f"$n.version"))(sources._1, sources._2)(f)
  }
  def makeAutoClosed[Type, Source, Version](n:String, source:Versioned[Source, Version])(f:Source=>Future[Type])(closer:Type => Unit) = {
    Make(contextTryStore[Type](n, Some(closer)),
         contextStore[Version](f"$n.version"))(source)(f)
  }
  def makeAutoClosed[Type, Source1, Version1, Source2, Version2]
        (n:String, sources:(Versioned[Source1, Version1], Versioned[Source2, Version2]))
        (f:((Source1, Source2))=>Future[Type])
        (closer:Type => Unit) = {
    Make(contextTryStore[Type](n, Some(closer)),
         contextStore[(Version1, Version2)](f"$n.version"))(sources._1, sources._2)(f)
  }

}

class FsDObject(c:MutableDContext, name:String, val dir:File) extends DObject(c, name) {

  val files = mutable.HashSet[File]()

  def allocFile(n:String) = {
    dir.mkdirs()
    val file = new File(dir, n)
    files += file
    file
  }

  def delete : Unit = {
    remove
    files.foreach( _.delete )
    dir.list().size == 0 && dir.delete()
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
  (n:String, source:Versioned[Source, Version])
  (f:Source=>Future[Type]) = {
    Make(fileTryStore[Type](n),
      contextAndFileStore[Version](f"$n.version"))(source)(f)
  }

  def makeFile[Type <: AnyRef, Source1, Version1, Source2, Version2]
    (n:String, sources:(Versioned[Source1, Version1], Versioned[Source2, Version2]))
    (f:((Source1, Source2))=>Future[Type]) = {
    Make(fileTryStore[Type](n),
         contextAndFileStore[(Version1, Version2)](f"$n.version"))(sources._1, sources._2)(f)
  }

  def makeWrittenFile[Type <: AnyRef, Source, Version]
  (n:String, source:Versioned[Source, Version])
  (f:(Source, File)=>Future[Unit]) = {
    val file = allocFile(n)
    Make(new FileNameTryStore(file),
         contextAndFileStore[Version](f"$n.version"))(source) { s =>
      f(s, file).map { v => file }
    }
  }


  def makePersistent[Type, Source, Version]
    (n:String, source:Versioned[Source, Version])
    (f:Source=>Future[Type]) = {
    Make(contextAndFileTryStore[Type](n),
         contextAndFileStore[Version](f"$n.version"))(source)(f)
  }

  def makePersistent[Type, Source1, Version1, Source2, Version2]
  (n:String, sources:(Versioned[Source1, Version1], Versioned[Source2, Version2]))
  (f:((Source1, Source2))=>Future[Type]) = {
    Make(contextAndFileTryStore[Type](n),
         contextAndFileStore[(Version1, Version2)](f"$n.version"))(sources._1, sources._2)(f)
  }

}
