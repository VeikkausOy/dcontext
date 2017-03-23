package fi.veikkaus.dcontext.store

import java.io._

import fi.veikkaus.dcontext.MutableDContext

import scala.ref.WeakReference
import scala.util.{Failure, Success, Try}


object IoUtil {
  def readAny(file:File) = {
    val in = new ObjectInputStream(new FileInputStream(file))
    try {
      in.readObject()
    } finally {
      in.close
    }
  }
  def read[T](file:File) : T = {
    readAny(file).asInstanceOf[T]
  }
  def atomicWrite[T](file:File, v:T) = {
    val tmpFile = new File(file.getParent, file.getName + ".tmp")
    val out = new ObjectOutputStream(new FileOutputStream(tmpFile))
    try {
      out.writeObject(v)
    } finally {
      out.close
    }
    tmpFile.renameTo(file)
  }
}

trait Store[T] {
  def get : Option[T]
  def update(t:Option[T])
  def isEmpty : Boolean = get.isEmpty
  def isDefined : Boolean = !isEmpty
  def delete = {}
}

case class FakeStore[T]() extends Store[T] {
  def get = None
  def update(t:Option[T]) {}
}

case class HeapStore[T](var value : Option[T] = None) extends Store[T] {
  def get = value
  def update(t:Option[T]): Unit = {
    value = t
  }
}
case class WeakStore[T <: AnyRef](var value : Option[WeakReference[T]] = None) extends Store[T] {
  def get : Option[T] = value.flatMap( _.get )
  def update(t:Option[T]): Unit = {
    value = t.map( WeakReference[T](_) )
  }
}

case class WeakTryStore[T <: AnyRef](var value : Option[Try[WeakReference[T]]] = None) extends Store[Try[T]] {
  def get : Option[Try[T]] = value.flatMap(_ match {
    case Success(v : WeakReference[T]) => v.get.map { Success(_) }
    case Failure(e) => Some(Failure(e))
  })
  def update(t:Option[Try[T]]): Unit = {
    value = t.map( _ match {
      case Success(v) => Success(WeakReference(v))
      case Failure(e) => Failure(e)
    })
  }
}
case class FileStore[T](file:File) extends Store[T] {
  override def delete: Unit = {
    file.delete
  }
  def get = {
    file.exists() match {
      case true =>
        try {
          Some(IoUtil.read[T](file))
        } catch {
          case e: Exception =>
            None
        }
      case false => None
    }
  }
  override def isEmpty = {
    !file.exists()
  }
  def update(t:Option[T]) = {
    t match {
      case None    => file.delete()
      case Some(v) => IoUtil.atomicWrite(file, v)
    }
  }
}

case class FileNameTryStore(file:File) extends Store[Try[File]] {
  val errorStore = new FileStore[Throwable](new File(file.getParentFile, file.getName + ".error"))
  override def delete: Unit = {
    errorStore.delete
    file.delete()
  }
  def get = {
    (errorStore.get, file.exists()) match {
      case (Some(err), _) => Some(Failure(err))
      case (None, true)   => Some(Success(file))
      case (None, false)  => None
    }
  }
  override def isEmpty = {
    !file.exists() && errorStore.isEmpty
  }
  def update(t:Option[Try[File]]) = {
    t match {
      case None    =>
        file.delete()
        errorStore.update(None)
      case Some(Failure(err)) =>
        file.delete // TODO: write error?
        errorStore.update(Some(err))
      case Some(Success(file)) => // already there
        if (file != this.file) throw new RuntimeException(this.file + " != " + file)
        errorStore.update(None)
    }
  }
}

case class FileTryStore[T](file:File) extends Store[Try[T]] {
  val errorStore = new FileStore[Throwable](new File(file.getParentFile, file.getName + ".error"))
  val store = new FileStore[T](file)
  override def delete: Unit = {
    errorStore.delete
    store.delete
  }
  override def isEmpty = {
    store.isEmpty && errorStore.isEmpty
  }
  def get = {
    (errorStore.get, store.get) match {
      case (Some(err), _) => Some(Failure(err))
      case (_, Some(v)) => Some(Success(v))
      case (None, None) => None
    }
  }
  def update(t:Option[Try[T]]) = {
    t match {
      case None             => errorStore.update(None); store.update(None)
      case Some(Failure(e)) => errorStore.update(Some(e)); store.update(None)
      case Some(Success(v)) => errorStore.update(None); store.update(Some(v))
    }
  }
}

/**
  * Filtered store will store None, if updated value doesn't match the filter
  */
case class FilteredTryStore[T](store:Store[Try[T]], filter : Try[T] => Boolean) extends Store[Try[T]] {
  def get : Option[Try[T]] = store.get
  def update(t:Option[Try[T]]) = {
    store.update(t.filter(filter))
  }
  override def isEmpty : Boolean = store.isEmpty
  override def delete = store.delete
}

case class CachedStore[T](cache:Store[T], box:Store[T]) extends Store[T] {
  override def delete: Unit = {
    cache.delete
    box.delete
  }
  override def isEmpty: Boolean = cache.isEmpty && box.isEmpty
  def get = {
    cache.get match {
      case None => {
        val rv = box.get
        cache.update(rv)
        rv
      }
      case Some(v) => Some(v)
    }
  }
  def update(t:Option[T]) = {
    box.update(t)
    cache.update(t)
  }
}

case class AutoClosedTryStore[T](store:Store[Try[T]], closer:T=>Unit) extends Store[Try[T]] with Closeable {
  override def close = get.map { _.map { closer } }
  def get = store.get
  def update(t:Option[Try[T]]) = {
    if (t != get) close
    store.update(t)
  }
  override def delete = {
    close
    store.delete
  }
}

case class ContextStore[T](c:MutableDContext, name:String, closer:Option[T => Unit] = None) extends Store[T] {
  def get = c.get[T](name)
  def update(t:Option[T]): Unit = {
    t match {
      case Some(v) =>
        c.put(name, v, closer.map(cl => new Closeable {
          override def close(): Unit = cl(v)
        }))
      case None => c.remove(name)
    }
  }
}

case class ContextTryStore[T](c:MutableDContext, name:String, closer:Option[T => Unit] = None) extends Store[Try[T]] {
  def get = c.get[Try[T]](name)
  def update(t:Option[Try[T]]): Unit = {
    t match {
      case Some(v) =>
        c.put(name, v, closer.map(cl => new Closeable {
          override def close(): Unit = v.foreach(cl(_))
        }))
      case None => c.remove(name)
    }
  }
}
