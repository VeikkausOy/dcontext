package fi.veikkaus.dcontext.value

import java.io.{Closeable, File}

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
class Guard[Value, Version](source:Versioned[Value, Version])
  extends Versioned[Value, Version] {

  private val logger = LoggerFactory.getLogger(classOf[Guard[Value, Version]])
  private var request : Option[(Option[Version], Future[Option[(Try[Value], Version)]])] = None

  override def updated(version: Option[Version]): Future[Option[(Try[Value], Version)]] = synchronized {
    logger.debug("updated called for " + Guard.this.hashCode())

    /*
     * Split behavior based on a request has already been requested before.
     * This is the segment, that implements the actul barrier logic
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
                                    build : Source => Future[Value])
  extends Versioned[Value, Version] {

  val guard = new Guard(source.mapWith( build(_) ))

  def updated(version:Option[Version]) = guard.updated(version)

}

/**
  * Make establishes a simple build system, that is used for tracking various
  * values, that are build based on data from mutating sources.
  *
  * Make wraps 'Build'-objet and provides one additional service:
  *
  *   1. Cache
  *   2.
  *
  */
class Make[Value, Source, Version](source : Versioned[Source, Version],
                                   build : Source => Future[Value],
                                   valueStore:Store[Try[Value]],
                                   versionStore:Store[Version])
  extends VersionedVal[Value, Version] {

  private val logger = LoggerFactory.getLogger(classOf[Make[Value, Source, Version]])

  def make(u:Option[(Try[Source], Version)]) : Future[Option[(Try[Value], Version)]] = u match {
    case None => Future { None }
    case Some((t, version)) if (Some(version) == versionStore.get && valueStore.isDefined) =>
      Future {
        Some((valueStore.get.get, version))
      }.recoverWith {
        case e =>
          logger.error("loading old value failed with " + e)
          logger.info("reseting old value")
          valueStore.update(None)
          versionStore.update(None)
          source.updated(Some(version)).flatMap(make)
      }
    case Some((t, version)) =>
      logger.info("source: " + t.hashCode() + ", version: " + version)
      t.map(v => build(v).map(Success(_)).recover { case err => Failure(err) })
        .recover { case err => Future { Failure(err) } }
        .get.map { t2 =>
        logger.info("build done.")
        synchronized {
          valueStore.update (Some (t2) )
          versionStore.update (
            Some (version).filter(e => valueStore.isDefined)
          )
        }
        Some((t2, version))
      }
  }

  val guard = new Guard(source.mapUpdateWith { make })

  def updated(version:Option[Version]) = guard.updated(version)

  def valueAndVersion : Future[(Try[Value], Version)] = {
    updated(versionStore.get).flatMap(
      _ match {
        case None if (valueStore.isDefined && versionStore.isDefined) =>
          logger.debug("getting recent version from store")
          val before = System.currentTimeMillis()
          val rv = Make.this.synchronized (valueStore.get.get, versionStore.get.get)
          logger.debug("loading took " + (System.currentTimeMillis() - before) + "ms")
          Future { rv }
        case None =>
          updated(None).map(_.get)
        case Some(v) =>
          Future { v }
      }
    )
  }

  /**
    * Gets the immediately available result, but launches an update
    * on the background. If no immediate value is available:
    * the method will block, until new once is build.
    */
  def fastValueAndVersion : Future[(Try[Value], Version)] = {
    (valueStore.get, versionStore.get) match {
      case (Some(value), Some(version)) =>
        updated(Some(version)) // update the value on the background
        Future { (value, version) }
      case _ => valueAndVersion
    }
  }

  def storedVersion = versionStore.get

  def getTry = valueAndVersion.map(_._1)
  def get = valueAndVersion.map(_._1.get)
  def getFast = fastValueAndVersion.map(_._1.get)
  def getTryFast = fastValueAndVersion.map(_._1)

}

object Make {

  def apply[Type, Source, Version](
      valueStore:Store[Try[Type]], versionStore:Store[Version])(
      s1:Versioned[Source, Version])(build : Source => Future[Type]) = {
    new Make[Type, Source, Version](s1, build, valueStore, versionStore)
  }

  def apply[Type, Source1, Version1, Source2, Version2](
     valueStore:Store[Try[Type]], versionStore:Store[(Version1, Version2)])(
     s1:Versioned[Source1, Version1], s2:Versioned[Source2, Version2])(build : ((Source1, Source2)) => Future[Type]) = {
    new Make[Type, (Source1, Source2), (Version1, Version2)](
      new VersionedPair[Source1, Version1, Source2, Version2](s1, s2),
      build,
      valueStore,
      versionStore)
  }
}