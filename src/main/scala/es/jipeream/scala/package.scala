package es.jipeream.scala

import scala.util.control.Exception._

package object typeutils {
  implicit class StringUtils(val s: String) {
    implicit def isInteger: Boolean = (allCatch opt s.toInt).isDefined

    implicit def toOptionInteger = if (s.isInteger) None else Some(s.toInt)

    implicit def isDouble: Boolean = (allCatch opt s.toDouble).isDefined

    implicit def toOptionDouble = if (s.isDouble) None else Some(s.toDouble)

  }
}
