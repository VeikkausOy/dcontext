package fi.veikkaus.dcontext.value

import fi.veikkaus.dcontext.MutableDContext
import fi.veikkaus.dcontext.store.Store
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * What is the actual need?
  *
  *   controller obsoletion
  *   memory behavior
  *
  * Which are the use cases?
  *
  *   getting from store
  *   recreating
  *
  * What are the usage alternatives?
  *
  *   object.get
  *
  *   object.updated(currentVersion) { data =>
  *
  *   }.getOrElse
  *
  *
  */


trait Versioned[Value, Version] {

  /**
    * Returns an updated version, if one exists
    * If there is no updated version, the future will return None
   */
  def updated(version:Option[Version]) : Future[Option[(Try[Value], Version)]]

  def map[Value2](f : Value => Value2) : Versioned[Value2, Version] = {
    def self = this
    new Versioned[Value2, Version] {
      override def updated(version: Option[Version]): Future[Option[(Try[Value2], Version)]] = {
        self.updated(version).map { _ map { case (t, v) =>
            (t.map(f), v)
          }
        }
      }
      override def toString = {
        f"Versioned(${Versioned.this}).map($f)"
      }
    }
  }

  def mapWith[Value2](f : Value => Future[Value2]) : Versioned[Value2, Version] = {
    def self = this
    new Versioned[Value2, Version] {
      override def updated(version: Option[Version]): Future[Option[(Try[Value2], Version)]] = {
        self.updated(version).flatMap { _ match {
          case Some((Success(value), version)) => f(value).map { value2 =>
            Some((Success(value2), version))
          }.recover { case err =>
            Some((Failure(err)), version)
          }
          case Some((Failure(err), version)) =>
            Future { Some((Failure(err), version)) }
          case None => Future { None }
          }
        }
      }
      override def toString = {
        f"Versioned(${Versioned.this}).mapWith($f)"
      }
    }
  }

  def mapTry[Value2](f : Try[Value] => Try[Value2]) = {
    def self = this
    new Versioned[Value2, Version] {
      override def updated(version: Option[Version]): Future[Option[(Try[Value2], Version)]] = {
        self.updated(version).map { _ map { case (t, v) =>
            (f(t), v)
          }
        }
      }
      override def toString = {
        f"Versioned(${Versioned.this}).mapTry($f)"
      }
    }
  }

  def mapUpdateWith[Value2](f : Option[(Try[Value], Version)] => Future[Option[(Try[Value2], Version)]]) = {
    def self = this
    new Versioned[Value2, Version] {
      override def updated(version: Option[Version]): Future[Option[(Try[Value2], Version)]] = {
        self.updated(version).flatMap { f }
      }
      override def toString = {
        f"Versioned(${Versioned.this}).mapUpdatedWith($f)"
      }
    }
  }

  def zip[Value2, Version2](v:Versioned[Value2, Version2]) = {
    new VersionedPair[Value, Version, Value2, Version2](this, v)
  }

  /** hack for dealing with oddities in Scala's type inference */
  def asVersioned = this

  def mapVersion[Version2](forward:Version=>Version2,
                           backward: (Version2) => Option[Version] = (v:Version2) => None) = {
    val self = this
    new Versioned[Value, Version2] {
      def updated(version: Option[Version2]): Future[Option[(Try[Value], Version2)]] = {
        self.updated(version.flatMap(backward)).map { _ match {
          case Some((value, v)) =>
            val newVersion = forward(v)
            if (Some(newVersion) == version)
              None // nothing has changed
            else
              Some((value, newVersion))
          case None => None
        }
        }
      }
    }
  }

  def mapVersionWith[Version2](f:Version=>Future[Version2]) = {
    val self = this
    new Versioned[Value, Version2] {
      def updated(version: Option[Version2]): Future[Option[(Try[Value], Version2)]] = {
        self.updated(None).flatMap { _ match {
          case Some((value, v)) =>
            f(v).map { newVersion =>
              if (Some(newVersion) == version)
                None // nothing has changed
              else
                Some((value, newVersion))
            }
          case None =>
            throw new RuntimeException(f"$self.mapVersionWith($f)/updated($version) encountered None")
        }
        }
      }
    }
  }

  /**
    * Returns a versioned object of form Versioned[Value, Value]
    */
  def valueAsVersion(failureVersion:Value) = {
    val self = this
    new Versioned[Value, Value] {
      def updated(version: Option[Value]): Future[Option[(Try[Value], Value)]] = {
        self.updated(None).map { _ match {
          case Some((_, v)) if Some(v) == version => None // nothing has changed
          case Some((Success(value), version)) => Some((Success(value), value))
          case Some((Failure(err), version))   => Some((Failure(err), failureVersion))
          }
        }
      }
    }
  }

  def get = updated(None).map(_.get._1.get)
  def getVersion = updated(None).map(_.get._2)

}

object Versioned {
  def apply[T, V](t: => T, v: => V) : Versioned[T, V] = new Versioned[T, V] {
    // returns an updated version, if one exists
    override def updated(version: Option[V]): Future[Option[(Try[T], V)]] = {
      Future {
        version match {
          case Some(vs) if vs == v => None
          case vs                  => Some(Success(t) -> v)
        }}
    }
    override def toString = {
      f"Versioned(<value>, $v)"
    }
  }
}

object Version {
  def apply[T](t: => T) = Versioned(t, t)
}

case class VersionedPair[TypeA, VersionA, TypeB, VersionB](a:Versioned[TypeA, VersionA],
                                                           b:Versioned[TypeB, VersionB])
  extends Versioned[(TypeA, TypeB), (VersionA, VersionB)] {

  private val logger = LoggerFactory.getLogger(getClass)

  private def zipTry(a:Try[TypeA], b:Try[TypeB]): Try[(TypeA, TypeB)] = {
    (a, b) match {
      case (Success(a), Success(b)) => Success((a, b))
      case (Failure(err), _) => Failure(err) // report only the first error
      case (_, Failure(err)) => Failure(err)
    }
  }
  // returns an updated version, if one exists
  override def updated(version: Option[(VersionA, VersionB)]) = {
//    logger.info(f"updated($version) for VersionedPair ${this.hashCode()}")
    a.updated(version.map(_._1)) zip b.updated(version.map(_._2)) flatMap {
      _ match {
        case (None, None) => Future {
          None
        }
        case (Some(ar), Some(br)) => Future {
          Some(zipTry(ar._1, br._1), (ar._2, br._2))
        }
        case (Some(ar), None) => // a had changes, let's get also b
          b.updated(None).map { _ match {
            case Some(br) =>
              Some(zipTry(ar._1, br._1), (ar._2, br._2))
            case None =>
              logger.error(b + " failed to provide new state for VersionedPair " + this.hashCode)
              throw new RuntimeException(b + " failed to provide new state for VersionedPair " + this.hashCode)
          }}
        case (None, Some(br)) => // b had changes, let's get also a
          a.updated(None).map { _ match {
            case Some(ar) =>
              Some(zipTry(ar._1, br._1), (ar._2, br._2))
            case None =>
              logger.error(a + " failed to provide new state " + this.hashCode)
              throw new RuntimeException(a + " failed to provide new state for VersionedPair " + this.hashCode)
          }}
      }
    }
  }
}

case class VersionedSeq[V, T, TV](seq:Versioned[Seq[Versioned[T, TV]], V])
  extends Versioned[Seq[T], (V, Option[Seq[TV]])] {
  private val logger = LoggerFactory.getLogger(getClass)

  // returns an updated version, if one exists
  override def updated(version: Option[(V, Option[Seq[TV]])]): Future[Option[(Try[Seq[T]], (V, Option[Seq[TV]]))]] = {
    // always fetch the most recent sequence version
    logger.info("updated(" + version + ") called")
    seq.updated(None).flatMap { case Some((trySeq, seqV)) =>
      logger.info("new version is " + seqV)
      trySeq match {
        case Failure(e) =>
          Future {
            Some((Failure(e), (seqV, None)))
          }
        case Success(seq) =>
          val versions = version match {
            case None => // we didn't have any versions
              None
            case Some((sv, Some(vvs))) if (sv == seqV) => // sequence is still the same
              Some(vvs)
            case _ => // sequence has changed
              None
          }
          logger.info("sequence size is " + seq.size)
          logger.info("requested subversions are " + versions)
          Future.sequence(
            seq.zipWithIndex.map(e => (e._1 : Versioned[T, TV], versions.map(_ (e._2)))).map { case (t, v) =>
              t.updated(v)
            }).flatMap { v =>
            if (v.exists(_.isDefined)) {
              // if any of the entries has changed, recreate all
              logger.info("current versions: " + v.map(_.map(_._2)).mkString(", "))
              Future.sequence(
                v.zipWithIndex.map { case (e, i) => e match {
                  case Some((t, v)) => Future {
                    (t, v)
                  }
                  case None => seq(i).updated(None).map(_.get) // force retrieval of value
                }
                }).map {
                _ match {
                  // fail, if there was any failure
                  case vs if vs.exists(_._1.isFailure) =>
                    val f = vs.find(_._1.isFailure).map(_._1.asInstanceOf[Failure[T]]).get
                    logger.info("failed with " + f.exception)
                    Some(Failure(f.exception) -> vs.map(_._2))
                      : Option[(Try[Seq[T]], Seq[TV])]
                  // success, all values retrieved for further processing
                  case vs =>
                    logger.info("succeeded.")
                    Some((Success(vs.map(_._1.get))) -> vs.map(_._2)): Option[(Try[Seq[T]], Seq[TV])]
                }
              }
            } else {
              logger.info("nothing had changed.")
              // nothing has changed, return success
              Future {
                None: Option[(Try[Seq[T]], Seq[TV])]
              }
            }
          }.map {
            _ match {

              case Some((ts, vs)) =>
                Some((ts, (seqV, Some(vs))))
              case None =>
                None // sequence version is the same, and all items are the same
            }
          }

      }

    }
  }
}

object VersionedSeq {
  def toVersioned[E <: Versioned[T, TV], T, TV](e:E) = e : Versioned[T, TV]

  def from[V, T, TV](seq:Versioned[Seq[_ <: Versioned[T, TV]], V]): Unit = {
    new VersionedSeq(seq.map[Seq[Versioned[T, TV]]](e => e.map(_.asInstanceOf[Versioned[T, TV]])))
  }
}

trait VersionedVal[T, V] extends Versioned[T, V] with AsyncVal[T] {

}

/**
  * Used mainly for testing
  */
class VersionedVar[T](v:Var[T], versionVar:Var[Long] = new HeapVar[Long](0)) extends VersionedVal[T, Long] {
  def version = versionVar.get

  override def updated(oldVersion: Option[Long]): Future[Option[(Try[T], Long)]] = {
    oldVersion match {
      case Some(v) if v == version => Future { None }
      case _ => Future { Some((Success(v.get), version)) }
    }
  }
  def update(value: T): Unit = {
    versionVar.update(version + 1)
    v.update(value)
  }
  override def get = Future { v.get }
  def apply() = v.get
}

class VersionedValImpl[T, V](v:Val[T], versionVal:Val[V]) extends VersionedVal[T, V] {
  def version = versionVal.get
  override def updated(oldVersion: Option[V]): Future[Option[(Try[T], V)]] = {
    oldVersion match {
      case Some(v) if v == version => Future { None }
      case _ => Future { Some((Success(v.get), version)) }
    }
  }
  override def get = Future { v.get }
}

object VersionedVal {
  def apply[Value, Version](value : => Value, version : => Version) = {
    new VersionedValImpl[Value, Version](Val(value), Val(version))
  }
}