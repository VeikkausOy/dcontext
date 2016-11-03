package fi.veikkaus.dcontext.value

import fi.veikkaus.dcontext.MutableDContext
import fi.veikkaus.dcontext.store.Store

import scala.concurrent.Future
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
  // returns an updated version, if one exists
  def updated(version:Option[Version]) : Future[Option[(Try[Value], Version)]]
}

class VersionedPair[TypeA, VersionA, TypeB, VersionB](a:Versioned[TypeA, VersionA],
                                                      b:Versioned[TypeB, VersionB])
  extends Versioned[(TypeA, TypeB), (VersionA, VersionB)] {

  private def zipTry(a:Try[TypeA], b:Try[TypeB]): Try[(TypeA, TypeB)] = {
    (a, b) match {
      case (Success(a), Success(b)) => Success((a, b))
      case (Failure(err), _) => Failure(err) // report only the first error
      case (_, Failure(err)) => Failure(err)
    }
  }
  // returns an updated version, if one exists
  override def updated(version: Option[(VersionA, VersionB)]) = {
    a.updated(version.map(_._1)) zip b.updated(version.map(_._2)) flatMap {
      _ match {
        case (None, None) => Future {
          None
        }
        case (Some(ar), Some(br)) => Future {
          Some(zipTry(ar._1, br._1), (ar._2, br._2))
        }
        case (Some(ar), None) => // a had changes, let's get also b
          b.updated(None).map { case Some(br) =>
            Some(zipTry(ar._1, br._1), (ar._2, br._2))
          }
        case (None, Some(br)) => // b had changes, let's get also a
          a.updated(None).map { case Some(ar) =>
            Some(zipTry(ar._1, br._1), (ar._2, br._2))
          }
      }
    }
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