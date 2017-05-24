package fi.veikkaus.dcontext.value

import java.io.{Closeable, File}
import scala.reflect.runtime.universe._

import fi.veikkaus.dcontext.store.Store
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
 * This method prevents more than one simultaneous update requests from passing to the source.
 *
 * If there are several requests, these updates will get merged.
 */
class Guard[Value, Version](source:Versioned[Value, Version])(implicit refs:ReferenceManagement[Value])
  extends Versioned[Value, Version] {

  private val logger = LoggerFactory.getLogger(classOf[Guard[Value, Version]])
  private var request : Option[(Option[Version], Future[Option[(Try[Value], Version)]])] = None

  override def updated(version: Option[Version]): Future[Option[(Try[Value], Version)]] = synchronized {
    logger.debug("updated called for " + Guard.this.hashCode())

    /*
     * Split behavior based on a request has already been requested before.
     * This is the segment, that implements the actual barrier logic
     */
    request match {
      case None =>
        logger.debug("new request for " + version)
        val f = source.updated(version).andThen { case res =>
          Guard.this.synchronized {
            request = None
          }
        }
        request = Some((version, f))
        f
      case Some((v, f)) =>
        logger.debug("request for " + v + " already pending")
        f.flatMap { _ match {
          case Some((value, newestV)) =>
            Future {
              if (version == Some(newestV)) {
                None
              } else {
                value.foreach { refs.inc } // additional value gets returned, it's caller's responsibility to clear it
                Some((value, newestV))
              }
            }
          case None =>
            if (version == v) { // v is the newest version, and this request has it, good
              Future { None }
            } else { // oh no, v is the newest version, but this request doesn't have it -> rerequest
              logger.debug("redo request")
              updated(version) // redo the request
            }
        }
      }
    }
  }

  def completed = synchronized {
    request match {
      case None => Future { Unit }
      case Some((v, f)) => f.map { v => Unit }
    }
  }

  def isUpdating = request.isDefined
}


/**
  * Build class does few things:
  *
  *    1. It creates a barricade, which prevents several simulaneous builds on the same object
  *    2. It manages source data versioning
  *    3. It manages errors in the source data.
  *       Errors may be non-deterministic (e.g. caused by bad network)
  *
  * Note: could this class be split into 2 parts:
  *
  *    A. barricade
  *    B. and the build function & error management
  */
class Build[Value, Source, Version](source : Versioned[Source, Version],
                                    build : Source => Future[Value])(
                                   implicit valueRefs : ReferenceManagement[Value],
                                   sourceRefs : ReferenceManagement[Source])  extends Versioned[Value, Version] {

  val guard =
    new Guard(source.mapWith { sourceRes =>
      build(sourceRes).map { value =>
        sourceRefs.dec(sourceRes)
        valueRefs.inc(value)
      }
    })(valueRefs)

  def updated(version:Option[Version]) = guard.updated(version)

}

trait ReferenceManagement[T] {
  def inc(v:T) : T
  def dec(v:T) : Unit

  def zip[E](r:ReferenceManagement[E]) =
    new TupleReferenceManagement[T, E]()(this, r)
}

object ReferenceManagement {
  implicit def implicits[T]/*(tag:TypeTag[T])*/ = {
/*    tag.tpe match {
      case TypeRef(_, Tuple2)
    }*/
    new DefaultReferenceManagement[T]
  }
}

case class DefaultReferenceManagement[T]() extends ReferenceManagement[T] {
  def inc(v:T) = v
  def dec(v:T) = Unit
}

class TupleReferenceManagement[A, B]()(
  implicit aRefs:ReferenceManagement[A],
           bRefs:ReferenceManagement[B])
  extends ReferenceManagement[(A, B)] {
  def inc(v:(A, B)) = {
    aRefs.inc(v._1)
    try {
      bRefs.inc(v._2)
      v
    } catch {
      case e : Exception =>
        aRefs.dec(v._1)
        throw e
    }
  }
  def dec(v:(A, B)) = {
    aRefs.dec(v._1)
    try {
      bRefs.dec(v._2)
    } catch {
      case e : Exception =>
        val logger = LoggerFactory.getLogger(getClass)
        // likely fatal
        logger.error("RESOURCE LEAK? decreasing reference failed", e)
        aRefs.inc(v._1)
        throw e
    }
  }
}

case class RefCounted[V <: Closeable](val value:V, val initCount:Int = 0) extends Closeable {
  @volatile var count = initCount
  def apply() = value
  def inc = synchronized  {
    count += 1
    this
  }
  def dec = close
  def open = synchronized  {
    count += 1
    value
  }
  override def hashCode(): Int = value.hashCode()
  def close = synchronized {
    if (count <= 0) throw new RuntimeException("cannot decrease ref count as it is already " + count)
    count -= 1
    if (count == 0) {
      value.close
    }
  }
  override def toString = value.toString
}

case class RefCountManagement[T <: Closeable]() extends ReferenceManagement[RefCounted[T]] {
  def inc(v:RefCounted[T]) = {
    v.inc
    v
  }
  def dec(v:RefCounted[T]) = {
    v.dec
  }
}

/**
  * Make establishes a simple build system, that is used for tracking various
  * values, that are build based on data from mutating sources.
  *
  * Make wraps 'Build'-object and provides few additional services:
  *
  *   1. Barrier to guarantee that build is only called once
  *   2. Cache with storages
  *   3. Versioning with cache, so that cache is only updated as needed
  *
  * NOTE on reference management:
  *
  *    1. It assumed that the reference is already increased..
  *       A) ...inside the build function
  *       B) ...and for the source
  *
  */
class Make[Value, Source, Version](val source : Versioned[Source, Version],
                                   val build : Source => Future[Value],
                                   val valueStore:Store[Try[Value]],
                                   val versionStore:Store[Version])(
                                   implicit valueRefs : ReferenceManagement[Value],
                                            sourceRefs : ReferenceManagement[Source])
  extends VersionedVal[Value, Version] {

  private val logger = LoggerFactory.getLogger(classOf[Make[Value, Source, Version]])

  /**Reference management, because Scala lacks autopointers. */
  def incValue(res:Option[(Try[Value], Version)]) = {
    res match {
      case Some((Success(value), _)) => valueRefs.inc(value)
      case _ =>
    }
    res
  }
  def decValue(res:Option[(Try[Value], Version)]) =
    res match {
      case Some((Success(value), _)) => valueRefs.dec(value)
      case _ =>
    }

  def decSource(res:Option[(Try[Source], Version)]) = {
    res match {
      case Some((Success(sourceValue), _)) => sourceRefs.dec(sourceValue)
      case _ =>
    }
  }

  def make(sourceResult:Option[(Try[Source], Version)]) : Future[Option[(Try[Value], Version)]] = {
    def buildAndSave(sourceValue:Try[Source], version:Version) = {
      logger.debug("source: " + sourceValue.hashCode() + ", version: " + version)
      sourceValue.map(v => build(v).map(Success(_)).recover { case err => Failure(err) })
        .recover { case err => Future {
          logger.error(f"make ${Make.this.hashCode()} version $version failed with $err", err)
          Failure(err) }
        }.get.map { t2 =>
        logger.debug("build done.")
        synchronized {
          valueStore.update (Some (t2) )
          versionStore.update (
            Some (version).filter(e => valueStore.isDefined)
          )
        }
        Some((t2, version))
      }
    }
    (sourceResult match {
      case None => Future { None }
      case Some((t, version)) if (Some(version) == versionStore.get && valueStore.isDefined) =>
        Future {
          Some((valueStore.get.get, version))
        }.recoverWith {
          case e =>
            logger.error("loading old value failed with " + e, e)
            logger.info("reseting old value")
            synchronized {
              valueStore.update(None)
              versionStore.update(None)
            }
            buildAndSave(t, version) // just go recursive
        }
      case Some((t, version)) => buildAndSave(t, version)
    }).map { rv =>
      // Do reference management, because ... Scala doesn't have RAII & autopointers
      decSource(sourceResult)
      incValue(rv)
    }
  }

  val guard = new Guard(source.mapUpdateWith { make })(valueRefs)

  def updated(version:Option[Version]) = guard.updated(version)

  def valueAndVersion : Future[(Try[Value], Version)] = {
    updated(versionStore.get).flatMap( res =>
      Make.this.synchronized {
        res match {
          case None if (valueStore.isDefined && versionStore.isDefined) =>
            logger.debug("getting recent version from store")
            val before = System.currentTimeMillis()
            val rv = (valueStore.get.get.map(valueRefs.inc), versionStore.get.get)
            logger.debug("loading took " + (System.currentTimeMillis() - before) + "ms")
            Future { rv }
          case None =>
            updated(None).map(_.get)
          case Some(v) =>
            Future { v }
        }
    })
  }

  def completed = guard.completed

  /**
    * Gets the immediately available result, but launches an update
    * on the background. If no immediate value is available:
    * the method will block, until new one is build.
    */
  def fastValueAndVersion : Future[(Try[Value], Version)] = synchronized {
    (valueStore.get, versionStore.get) match {
      case (Some(value), Some(version)) =>
        val rv = ((value.map(valueRefs.inc), version))
        updated(Some(version)).foreach { res =>
          decValue(res)
        }
        Future { rv }
      case _ => valueAndVersion
    }
  }

  def storedVersion = versionStore.get
  def storedTry = valueStore.get

  def getTry = valueAndVersion.map(_._1)
  override def get = valueAndVersion.map(_._1.get)
  def getFast = fastValueAndVersion.map(_._1.get)
  def getTryFast = fastValueAndVersion.map(_._1)

  def isUpdating = guard.isUpdating

}

object Make {

  def apply[Type, Source, Version](
      valueStore:Store[Try[Type]], versionStore:Store[Version])(
      s1:Versioned[Source, Version])
      (build : Source => Future[Type])
      (implicit valueRefs : ReferenceManagement[Type], sourceRefs : ReferenceManagement[Source]) = {
    new Make[Type, Source, Version](s1, build, valueStore, versionStore)
  }
}